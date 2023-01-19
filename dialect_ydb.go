package pop

import (
	"bytes"
	"fmt"
	"io"
	"net/url"
	"os/exec"
	"strings"

	"github.com/gobuffalo/fizz"
	"github.com/gobuffalo/fizz/translators"
	"github.com/gobuffalo/pop/v6/columns"
	"github.com/gobuffalo/pop/v6/internal/defaults"
	"github.com/gobuffalo/pop/v6/logging"
	"github.com/jmoiron/sqlx"
)

const nameYDB = "ydb"
const hostYDB = "localhost"
const portYDB = "2135"

func init() {
	AvailableDialects = append(AvailableDialects, nameYDB)
	dialectSynonyms["YDB"] = nameYDB
	urlParser[nameYDB] = urlParserYDB
	finalizer[nameYDB] = finalizerYDB
	newConnection[nameYDB] = newYDB
}

type YDB struct {
	commonDialect
}

func (m *YDB) Name() string {
	return nameYDB
}

func (m *YDB) DefaultDriver() string {
	return nameYDB
}

func (*YDB) Quote(key string) string {
	return fmt.Sprintf("`%s`", key)
}

func (m *YDB) Details() *ConnectionDetails {
	return m.ConnectionDetails
}

func (m *YDB) URL() string {
	cd := m.ConnectionDetails
	if cd.URL != "" {
		return cd.URL
	}
	s := "grpcs://%s:%s@%s:%s/%s"
	return fmt.Sprintf(s, cd.User, cd.Password, cd.Host, cd.Port, cd.Database)
}

func (m *YDB) urlWithoutDb() string {
	cd := m.ConnectionDetails
	if replaced := strings.Replace(m.URL(), "/"+cd.Database+"?", "/?", 1); replaced == m.URL() {
		return strings.Replace(m.URL(), "/"+cd.Database, "", 1)
	} else {
		return replaced
	}
}

func (m *YDB) MigrationURL() string {
	return m.URL()
}

func (m *YDB) Create(c *Connection, model *Model, cols columns.Columns) error {
	if err := genericCreate(c, model, cols, m); err != nil {
		return fmt.Errorf("YDB create: %w", err)
	}
	return nil
}

func (m *YDB) Update(c *Connection, model *Model, cols columns.Columns) error {
	if err := genericUpdate(c, model, cols, m); err != nil {
		return fmt.Errorf("YDB update: %w", err)
	}
	return nil
}

func (m *YDB) UpdateQuery(c *Connection, model *Model, cols columns.Columns, query Query) (int64, error) {
	if n, err := genericUpdateQuery(c, model, cols, m, query, sqlx.QUESTION); err != nil {
		return n, fmt.Errorf("YDB update query: %w", err)
	} else {
		return n, nil
	}
}

func (m *YDB) Destroy(c *Connection, model *Model) error {
	stmt := fmt.Sprintf("DELETE FROM %s WHERE %s = *", m.Quote(model.TableName()), model.IDField())
	_, err := genericExec(c, stmt, model.ID())
	if err != nil {
		return fmt.Errorf("YDB destroy: %w", err)
	}
	return nil
}

func (m *YDB) Delete(c *Connection, model *Model, query Query) error {
	sqlQuery, args := query.ToSQL(model)
	// * YDB does not support table alias for DELETE syntax until 8.0.
	// * Do not generate SQL manually if they may have `WHERE IN`.
	// * Spaces are intentionally added to make it easy to see on the log.
	sqlQuery = asRegex.ReplaceAllString(sqlQuery, "  ")

	_, err := genericExec(c, sqlQuery, args...)
	return err
}

func (m *YDB) SelectOne(c *Connection, model *Model, query Query) error {
	if err := genericSelectOne(c, model, query); err != nil {
		return fmt.Errorf("YDB select one: %w", err)
	}
	return nil
}

func (m *YDB) SelectMany(c *Connection, models *Model, query Query) error {
	if err := genericSelectMany(c, models, query); err != nil {
		return fmt.Errorf("YDB select many: %w", err)
	}
	return nil
}

// CreateDB creates a new database, from the given connection credentials
func (m *YDB) CreateDB() error {
	deets := m.ConnectionDetails
	fmt.Printf("Creating: %+v", deets)
	query := exec.Command("/ydbd", m.urlWithoutDb(), "admin database /Root/testdb create")
	err := query.Run()
	if err != nil {
		return fmt.Errorf("error creating YDB database %s: %w", deets.Database, err)
	}

	log(logging.Info, "created database %s", deets.Database)
	return nil
}

// DropDB drops an existing database, from the given connection credentials
func (m *YDB) DropDB() error {
	deets := m.ConnectionDetails
	query := exec.Command("/ydbd", m.urlWithoutDb(), "admin database /Root/testdb remove")
	err := query.Run()

	if err != nil {
		return fmt.Errorf("error dropping YDB database %s: %w", deets.Database, err)
	}

	log(logging.Info, "dropped database %s", deets.Database)
	return nil
}

func (m *YDB) TranslateSQL(sql string) string {
	return sql
}

func (m *YDB) FizzTranslator() fizz.Translator {
	t := translators.NewYDB()
	return t
}

func (m *YDB) DumpSchema(w io.Writer) error {
	cmd := exec.Command("ydb", "-e", "grpc://localhost:2136", "-d", "/local", "tools", "dump")
	println(cmd.String())
	return genericDumpSchema(m.Details(), cmd, w)
}

// LoadSchema executes a schema sql file against the configured database.
func (m *YDB) LoadSchema(r io.Reader) error {
	return genericLoadSchema(m, r)
}

// TruncateAll truncates all tables for the given connection.
func (m *YDB) TruncateAll(tx *Connection) error {
	var stmts []string
	err := tx.RawQuery(YDBTruncate, m.Details().Database, tx.MigrationTableName()).All(&stmts)
	if err != nil {
		return err
	}
	if len(stmts) == 0 {
		return nil
	}

	var qb bytes.Buffer
	// #49: Disable foreign keys before truncation
	qb.WriteString("SET SESSION FOREIGN_KEY_CHECKS = 0; ")
	qb.WriteString(strings.Join(stmts, " "))
	// #49: Re-enable foreign keys after truncation
	qb.WriteString(" SET SESSION FOREIGN_KEY_CHECKS = 1;")

	return tx.RawQuery(qb.String()).Exec()
}

func newYDB(deets *ConnectionDetails) (dialect, error) {
	cd := &YDB{
		commonDialect: commonDialect{ConnectionDetails: deets},
	}
	return cd, nil
}

func urlParserYDB(cd *ConnectionDetails) error {
	db := strings.TrimPrefix(cd.URL, "grpc://")
	db = strings.TrimPrefix(db, "grpcs://")

	parts := strings.Split(db, "?")
	prefixParts := strings.Split(db, "@")
	if len(prefixParts) > 1 {
		credParts := strings.Split(prefixParts[0], ":")
		cd.User = credParts[0]
		cd.Password = credParts[1]
		prefixParts = prefixParts[1:]
	}
	prefixParts = strings.Split(prefixParts[0], "/")
	if len(prefixParts) > 1 {
		cd.Database = prefixParts[1]
	}
	prefixParts = strings.Split(prefixParts[0], ":")
	cd.Host = prefixParts[0]
	cd.Port = prefixParts[1]

	if len(parts) != 2 {
		return nil
	}

	params, err := url.ParseQuery(parts[1])
	if err != nil {
		return fmt.Errorf("unable to parse YDB query: %w", err)
	}

	for param := range params {
		cd.setOption(param, params.Get(param))
	}

	return nil
}

func finalizerYDB(cd *ConnectionDetails) {
	cd.Host = defaults.String(cd.Host, hostYDB)
	cd.Port = defaults.String(cd.Port, portYDB)

	defs := map[string]string{
		"readTimeout": "3s",
		"collation":   "utf8mb4_general_ci",
	}
	forced := map[string]string{
		"parseTime":       "true",
		"multiStatements": "true",
	}

	for k, def := range defs {
		cd.setOptionWithDefault(k, cd.option(k), def)
	}

	for k, v := range forced {
		// respect user specified options but print warning!
		cd.setOptionWithDefault(k, cd.option(k), v)
		if cd.option(k) != v { // when user-defined option exists
			log(logging.Warn, "IMPORTANT! '%s: %s' option is required to work properly but your current setting is '%v: %v'.", k, v, k, cd.option(k))
			log(logging.Warn, "It is highly recommended to remove '%v: %v' option from your config!", k, cd.option(k))
		} // or override with `cd.Options[k] = v`?
		if cd.URL != "" && !strings.Contains(cd.URL, k+"="+v) {
			log(logging.Warn, "IMPORTANT! '%s=%s' option is required to work properly. Please add it to the database URL in the config!", k, v)
		} // or fix user specified url?
	}
}

const YDBTruncate = "SELECT concat('TRUNCATE TABLE `', TABLE_NAME, '`;') as stmt FROM INFORMATION_SCHEMA.TABLES WHERE table_schema = ? AND table_name <> ? AND table_type <> 'VIEW'"
