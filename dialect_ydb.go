package pop

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/gobuffalo/fizz"
	"github.com/gobuffalo/fizz/translators"
	"github.com/gobuffalo/pop/v6/columns"
	"github.com/gobuffalo/pop/v6/internal/defaults"
	"github.com/gobuffalo/pop/v6/logging"
	"github.com/gofrs/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/ory/x/sqlxx"
	_ "github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

const NameYDB = "ydb"
const hostYDB = "localhost"
const portYDB = "8765"

var ErrUnimplementedInYdb = errors.New("YDB doesn't support this feature yet")

func init() {
	AvailableDialects = append(AvailableDialects, NameYDB)

	dialectSynonyms["ydb"] = NameYDB

	dialectSynonyms["ydb3"] = NameYDB
	dialectSynonyms["ydb/3"] = NameYDB
	dialectSynonyms["grpc"] = NameYDB

	finalizer[NameYDB] = finalizerYDB
	newConnection[NameYDB] = newYdb
}

var _ dialect = &ydb{}

type ydb struct {
	commonDialect
}

func (ydb) Quote(key string) string {
	return fmt.Sprintf("`%s`", key)
}

func (y *ydb) Name() string {
	return NameYDB
}

func (y *ydb) DefaultDriver() string {
	return "ydb"
}

func (y *ydb) Details() *ConnectionDetails {
	return y.ConnectionDetails
}

func (y *ydb) Create(c *Connection, model *Model, cols columns.Columns) error {
	keyType, err := model.PrimaryKeyType()
	if err != nil {
		return err
	}
	switch keyType {
	case "int", "int64":
		var id int64
		cols.Remove(model.IDField())
		w := cols.Writeable()
		if len(w.Cols) <= 0 {
			return ErrUnimplementedInYdb
		}
		query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s) RETURNING %s", y.Quote(model.TableName()), w.QuotedString(y), w.SymbolizedString(), model.IDField())
		txlog(logging.SQL, c, query, model.Value)

		q, args, err := sqlx.Named(query, model.Value)
		if err != nil {
			return err
		}
		q = sqlx.Rebind(sqlx.DOLLAR, q)
		for i := range args {
			//needed for the sake of successful work of YDB driver, because ydb driver doesn't parse sql.Null types well

			args[i], err = convertGoTypeToAppropriateYdb(args[i])
			if err != nil {
				return err
			}
		}

		rows, err := c.Store.QueryxContext(model.ctx, q, args...)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			err = rows.Scan(&id)
			if err != nil {
				return err
			}
			break
		}
		model.setID(id)
		return nil
	case "UUID", "string":
		if keyType == "UUID" {
			if model.ID() == emptyUUID {
				u, err := uuid.NewV4()
				if err != nil {
					return err
				}
				model.setID(u)
			}
		} else if model.ID() == "" {
			return fmt.Errorf("missing ID value")
		}
		w := cols.Writeable()
		w.Add(model.IDField())
		query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", y.Quote(model.TableName()), w.QuotedString(y), w.SymbolizedString())
		txlog(logging.SQL, c, query, model.Value)
		if _, err = NamedExecContext(model.ctx, c.Store, query, model.Value); err != nil {
			return fmt.Errorf("named insert: %w", err)
		}
		return nil
	}
	return fmt.Errorf("can not use %s as a primary key type!", keyType)
}

func convertGoTypeToAppropriateYdb(value interface{}) (interface{}, error) {
	if valuer, ok := value.(driver.Valuer); ok {
		if res, ok := value.(uuid.NullUUID); ok {
			if !res.Valid {
				return (*string)(nil), nil
			}
		}

		if res, ok := value.(sqlxx.JSONRawMessage); ok {
			tmp, _ := res.Value()
			return types.JSONDocumentValue(tmp.(string)), nil
		}

		if res, ok := value.(sqlxx.NullJSONRawMessage); ok {
			val, _ := res.Value()
			if str, ok := val.(*string); ok && str == nil {
				return types.NullableJSONDocumentValue((*string)(nil)), nil
			}
			tmp := val.(string)
			return types.NullableJSONDocumentValue(&tmp), nil
		}

		if res, ok := value.(*uuid.NullUUID); ok {
			if res == nil || !res.Valid {
				return (*string)(nil), nil
			}
		}

		if res, ok := value.(*sqlxx.JSONRawMessage); ok {
			if value == nil {
				return types.JSONDocumentValue(""), nil
			}
			tmp, _ := res.Value()
			return types.JSONDocumentValue(tmp.(string)), nil
		}

		if res, ok := value.(*sqlxx.NullJSONRawMessage); ok {
			if value == nil {
				return types.NullableJSONDocumentValue((*string)(nil)), nil
			}
			val, _ := res.Value()
			if str, ok := val.(*string); ok && str == nil {
				return types.NullableJSONDocumentValue((*string)(nil)), nil
			}
			tmp := val.(string)
			return types.NullableJSONDocumentValue(&tmp), nil
		}

		if v, skip := skipNilValues(value); skip {
			return v, nil
		}

		if v, changed := sanitizeTime(value); changed {
			return v, nil
		}

		if v, changed, err := sanitizeJsonRaw(value); err != nil {
			return nil, err
		} else if changed {
			return v, nil
		}

		valFromValuer, err := valuer.Value()
		if err != nil {
			return nil, err
		}

		//if not null then return it without transformations
		if valFromValuer != nil {
			return value, nil
		}
		switch value.(type) {
		case sql.NullBool, sqlxx.NullBool:
			return (*bool)(nil), nil
		case sql.NullString, sqlxx.NullString:
			return (*string)(nil), nil
		case sql.NullFloat64:
			return (*float64)(nil), nil
		case sql.NullInt16:
			return (*int16)(nil), nil
		case sql.NullInt32:
			return (*int32)(nil), nil
		case sql.NullInt64, sqlxx.NullInt64:
			return (*int64)(nil), nil
		case sql.NullByte:
			return (*byte)(nil), nil
		default:
			return value, nil
		}
	}
	if v, changed := sanitizeTime(value); changed {
		return v, nil
	}
	return value, nil
}

func sanitizeJsonRaw(value interface{}) (interface{}, bool, error) {
	switch v := value.(type) {
	case json.RawMessage:
		jsonBody, err := v.MarshalJSON()
		if err != nil {
			return nil, false, err
		}
		return types.JSONDocumentValueFromBytes(jsonBody), true, nil
	case *json.RawMessage:
		jsonBody, err := v.MarshalJSON()
		if err != nil {
			return nil, false, err
		}
		return types.JSONDocumentValueFromBytes(jsonBody), true, nil
	}
	return nil, false, nil
}

func sanitizeTime(value interface{}) (interface{}, bool) {
	timeStub := time.Now()
	transform := func(value interface{}) (interface{}, bool) {
		tmp, err := value.(driver.Valuer).Value()
		if tmp == nil {
			return (*time.Time)(nil), true
		}
		if err == nil {
			potentialZero1, ok := tmp.(time.Time)
			if ok && potentialZero1.IsZero() {
				return timeStub, true
			}
			potentialZero2, ok := tmp.(*time.Time)
			if ok && (potentialZero2 == nil || potentialZero2.IsZero()) {
				return timeStub, true
			}
		}
		return nil, false
	}
	switch v := value.(type) {
	case *sql.NullTime:
		if v == nil {
			return (*time.Time)(nil), true
		}
		tmp, changed := transform(value)
		if changed {
			return tmp, true
		}
	case *sqlxx.NullTime:
		if v == nil {
			return (*time.Time)(nil), true
		}
		tmp, changed := transform(value)
		if changed {
			return tmp, true
		}
	case sql.NullTime, sqlxx.NullTime:
		tmp, changed := transform(value)
		if changed {
			return tmp, true
		}
	case *time.Time:
		if v == nil {
			return (*time.Time)(nil), true
		}
		if v.IsZero() {
			return timeStub, true
		}
	case time.Time:
		if v.IsZero() {
			return timeStub, true
		}
	}
	return nil, false
}

func skipNilValues(value interface{}) (interface{}, bool) {
	switch v := value.(type) {
	case *sql.NullBool:
		if v == nil {
			return (*bool)(nil), true
		}
	case *sqlxx.NullBool:
		if v == nil {
			return (*bool)(nil), true
		}
	case *sql.NullString:
		if v == nil {
			return (*string)(nil), true
		}
	case *sqlxx.NullString:
		if v == nil {
			return (*string)(nil), true
		}
	case *sql.NullFloat64:
		if v == nil {
			return (*float64)(nil), true
		}
	case *sql.NullTime:
		if v == nil {
			return (*time.Time)(nil), true
		}
	case *sqlxx.NullTime:
		if v == nil {
			return (*time.Time)(nil), true
		}
	case *sql.NullInt16:
		if v == nil {
			return (*int16)(nil), true
		}
	case *sql.NullInt32:
		if v == nil {
			return (*int32)(nil), true
		}
	case *sql.NullInt64:
		if v == nil {
			return (*int64)(nil), true
		}
	case *sqlxx.NullInt64:
		if v == nil {
			return (*int64)(nil), true
		}
	case *sql.NullByte:
		if v == nil {
			return (*byte)(nil), true
		}
	}
	return nil, false
}

func NamedSetupYdb(query string, model interface{}) (string, []interface{}, error) {
	q, args, err := sqlx.Named(query, model)
	if err != nil {
		return "", nil, err
	}
	q = sqlx.Rebind(sqlx.DOLLAR, q)
	for i := range args {
		args[i], err = convertGoTypeToAppropriateYdb(args[i])
		if err != nil {
			return "", nil, err
		}
	}
	return q, args, nil
}

func SimpleSetup(query string, args ...interface{}) (string, []interface{}, error) {
	q := sqlx.Rebind(sqlx.DOLLAR, query)
	var err error
	for i := range args {
		args[i], err = convertGoTypeToAppropriateYdb(args[i])
		if err != nil {
			return "", nil, err
		}
	}
	return q, args, nil
}

func NamedExecContext(ctx context.Context, c store, query string, model interface{}) (sql.Result, error) {
	q, args, err := NamedSetupYdb(query, model)
	if err != nil {
		return nil, err
	}
	return c.ExecContext(ctx, q, args...)
}

func (y *ydb) Update(connection *Connection, model *Model, columns columns.Columns) error {
	stmt := fmt.Sprintf("UPDATE %s SET %s WHERE %s;", y.Quote(model.TableName()), columns.Writeable().QuotedUpdateString(y), model.WhereNamedIDWithTableName())
	txlog(logging.SQL, connection, stmt, model.ID())
	_, err := NamedExecContext(model.ctx, connection.Store, stmt, model.Value)
	if err != nil {
		return err
	}
	return nil
}

func (y *ydb) UpdateQuery(connection *Connection, model *Model, columns columns.Columns, query Query) (int64, error) {
	q := fmt.Sprintf("UPDATE %s SET %s", y.Quote(model.TableName()), columns.Writeable().QuotedUpdateString(y))

	q, updateArgs, err := sqlx.Named(q, model.Value)
	if err != nil {
		return 0, err
	}

	sb := query.toSQLBuilder(model, connection.Dialect.Name())
	q = sb.buildWhereClauses(q)

	q = sqlx.Rebind(sqlx.DOLLAR, q)

	q = fmt.Sprintf("%s returning %s", q, model.IDField())
	allArgs := append(updateArgs, sb.args...)
	txlog(logging.SQL, connection, q, allArgs...)
	rows, err := connection.Store.QueryxContext(connection.Context(), q, allArgs...)
	if err != nil {
		return 0, err
	}

	var count int64
	for rows.Next() {
		count++
	}

	return count, nil
}

func (y *ydb) Destroy(connection *Connection, model *Model) error {
	stmt := y.TranslateSQL(fmt.Sprintf("DELETE FROM %s WHERE %s;", y.Quote(model.TableName()), model.WhereID()))
	_, err := genericExec(connection, stmt, model.ID())
	if err != nil {
		return err
	}
	return nil
}

func (y *ydb) Delete(connection *Connection, model *Model, query Query) error {
	return genericDelete(connection, model, query)
}

func (y *ydb) SelectOne(connection *Connection, model *Model, query Query) error {
	return genericSelectOne(connection, model, query)
}

func (y *ydb) SelectMany(connection *Connection, model *Model, query Query) error {
	return genericSelectMany(connection, model, query)
}

func (y *ydb) CreateDB() error {
	txlog(logging.SQL, "", "attempt to call CreateDB in YDB dialect module: Unimplemented")
	return nil
}

func (y *ydb) DropDB() error {
	txlog(logging.SQL, "", "attempt to call DropDB in YDB dialect module: Unimplemented")
	return nil
}

func (y *ydb) URL() string {
	//"grpc://login:password@localhost:2136/local?go_query_bind=numeric,declare,table_path_prefix(path/to/tables)"

	c := y.ConnectionDetails
	if c.URL != "" {
		return c.URL
	}
	s := "grpc://%s:%s@%s:%s/%s?%s"

	return fmt.Sprintf(s, c.User, url.QueryEscape(c.Password), c.Host, c.Port, c.Database, c.OptionsString(""))
}

func (y *ydb) MigrationURL() string {
	return y.URL()
}

func (y *ydb) TranslateSQL(sql string) string {
	return sqlx.Rebind(sqlx.DOLLAR, sql)
}

func (y *ydb) FizzTranslator() fizz.Translator {
	return translators.NewYdb(y.URL())
}

func (y *ydb) DumpSchema(_ io.Writer) error {
	txlog(logging.SQL, "", "attempt to call DumpSchema in YDB dialect module: Unimplemented")
	return nil
}

func (y *ydb) LoadSchema(_ io.Reader) error {
	txlog(logging.SQL, "", "attempt to call LoadSchema in YDB dialect module: Unimplemented")
	return nil
}

func (y *ydb) TruncateAll(connection *Connection) error {
	ctx := context.Background()
	res, err := connection.Store.QueryxContext(ctx, "SELECT Path FROM `.sys/partition_stats`;")
	if err != nil {
		return err
	}
	defer res.Close()

	var tables []string
	for res.Next() {
		var fullPath string
		err = res.Scan(&fullPath)
		if err != nil {
			return err
		}
		//if the table is a migration table
		if strContains(fullPath, connection.MigrationTableName()) {
			continue
		}
		//if the table is a service table
		pattern := regexp.MustCompile(`/\.[a-zA-Z0-9]+`)
		if pattern.MatchString(fullPath) {
			continue
		}
		//if the table doesn't belong to the current user space
		if !strContains(fullPath, y.ConnectionDetails.Database) {
			continue
		}

		tables = append(tables, fullPath)
	}

	newConn, err := sql.Open(NameYDB, y.URL())
	if err != nil {
		return err
	}
	defer newConn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	//in the beginning we should get rid of indices and only after that do tables
	for _, entityName := range tables {
		if strContains(entityName, "indexImplTable") {
			//get the tableName of current index
			indexName := filepath.Dir(entityName)
			tableName := filepath.Dir(indexName)
			_, err = newConn.ExecContext(ctx, fmt.Sprintf("ALTER TABLE `%s` DROP INDEX `%s`;", tableName, indexName))
			if err != nil {
				return err
			}
		}
	}

	for _, tableName := range tables {
		if !strContains(tableName, "indexImplTable") {
			_, err = newConn.ExecContext(ctx, fmt.Sprintf("DROP TABLE `%s`;", tableName))
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func newYdb(deets *ConnectionDetails) (dialect, error) {
	cd := &ydb{
		commonDialect: commonDialect{ConnectionDetails: deets},
	}
	return cd, nil
}

func finalizerYDB(cd *ConnectionDetails) {
	cd.Host = defaults.String(cd.Host, hostYDB)
	cd.Port = defaults.String(cd.Port, portYDB)
}

func strContains(inputStr string, subStr string) bool {
	if strings.Index(inputStr, subStr) == -1 {
		return false
	}
	return true
}

func ExecuteYqlOpSeparately(conn *Connection, sql string) error {
	ops := strings.Split(sql, ";")
	for _, op := range ops {
		_, err := conn.Store.Exec(op + ";")
		if err != nil {
			return err
		}
	}
	return nil
}
