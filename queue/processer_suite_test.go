// +build integration

package queue

import (
	"database/sql"
	"fmt"
	"log"
	"logger/helpers/test"
	"os"
	"regulus/mrs"
	"testing"

	. "engine/helpers/test"
	_ "github.com/lib/pq"
	. "logger/interfaces"
	. "skat/helpers/test"

	"github.com/stretchr/testify/suite"
)

const (
	SCHEMA_NAME_P = "queue"
	SCHEMA_PATH_P = "../migrations/schema.sql"
	DB_NAME_P     = "skat_test_db_suite_processer"
)

type ProcesserSuite struct {
	suite.Suite
	DBM        *mrs.DBM
	Logger     LogManager
	DbTemplate string
	DbName     string
}

// Иначе go test не запустит интеграционные тесты
func Test_Processer_Scenarious(t *testing.T) {
	ProcesserSuite := &ProcesserSuite{Logger: test.NewLogger(t)}
	user, pass, uri := TestPostgresCreds()
	ProcesserSuite.DbTemplate = "postgres://" + user + ":" + pass + uri + "%s?sslmode=disable"
	ProcesserSuite.DbName = DB_NAME_P + os.Getenv("BUILD_NUMBER")

	defer DropDB(fmt.Sprintf(ProcesserSuite.DbTemplate, "postgres"), ProcesserSuite.DbName)

	err := CreateOrResetDB(ProcesserSuite.DbTemplate, ProcesserSuite.DbName, user, SCHEMA_PATH_P, SCHEMA_NAME_P)
	if err != nil {
		t.Fatal(err)
	}

	suite.Run(t, ProcesserSuite)
}

func (s *ProcesserSuite) SetupSuite() {
	db, err := sql.Open("postgres", fmt.Sprintf(s.DbTemplate+"&search_path=%s", s.DbName, SCHEMA_NAME_P))
	if err != nil {
		log.Fatal(err)
	}

	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(16)

	s.DBM = mrs.NewDBM(db)

	ResetDB(s.DBM.DBH(s.Logger.NewLogger("", "")))
}

func (s *ProcesserSuite) SetupTest() {
	ResetDB(s.DBM.DBH(s.Logger.NewLogger("", "")))
}

func (s *ProcesserSuite) TearDownTest() {
	ResetDB(s.DBM.DBH(s.Logger.NewLogger("", "")))
}
