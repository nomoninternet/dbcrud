package dbunit

import (
	"database/sql"
	"fmt"
	"os"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	"github.com/joho/godotenv"
)

type UnitTest struct {
	t  *testing.T
	db *sql.DB
}

func (unit *UnitTest) New(t *testing.T) *sql.DB {

	// We need the path for this file to load the correct env settings
	//_, filename, _, _ := runtime.Caller(0)
	//path := path.Dir(filename)
	godotenv.Load(".env")

	var err error
	unit.db, err = sql.Open("mysql", os.Getenv("DBUNIT_DB_DSN"))
	if err != nil {
		panic(err)
	}

	return unit.db
}

func (unit *UnitTest) ClearTable(table string) {
	_, err := unit.db.Exec("DELETE FROM `" + table + "`")
	if err != nil {
		fmt.Print("Unable to delete contents of test table: %w", err)

	}
}
