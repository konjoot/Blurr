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
	SCHEMA_NAME = "queue"
	SCHEMA_PATH = "../migrations/schema.sql"
	DB_NAME     = "skat_test_db_suite_queue"
)

type QueueSuite struct {
	suite.Suite
	DBM        *mrs.DBM
	Logger     LogManager
	DbTemplate string
	DbName     string
}

// Иначе go test не запустит интеграционные тесты
func Test_Scenarious(t *testing.T) {
	QueueSuite := &QueueSuite{Logger: test.NewLogger(t)}
	user, pass, uri := TestPostgresCreds()
	QueueSuite.DbTemplate = "postgres://" + user + ":" + pass + uri + "%s?sslmode=disable"
	QueueSuite.DbName = DB_NAME + os.Getenv("BUILD_NUMBER")

	defer DropDB(fmt.Sprintf(QueueSuite.DbTemplate, "postgres"), QueueSuite.DbName)

	err := CreateOrResetDB(QueueSuite.DbTemplate, QueueSuite.DbName, user, SCHEMA_PATH, SCHEMA_NAME)
	if err != nil {
		t.Fatal(err)
	}

	suite.Run(t, QueueSuite)
}

func (s *QueueSuite) SetupSuite() {
	db, err := sql.Open("postgres", fmt.Sprintf(s.DbTemplate+"&search_path=%s", s.DbName, SCHEMA_NAME))
	if err != nil {
		log.Fatal(err)
	}

	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(16)

	s.DBM = mrs.NewDBM(db)

	ResetDB(s.DBM.DBH(s.Logger.NewLogger("", "")))
}

func (s *QueueSuite) SetupTest() {
	ResetDB(s.DBM.DBH(s.Logger.NewLogger("", "")))
}

func (s *QueueSuite) TearDownTest() {
	ResetDB(s.DBM.DBH(s.Logger.NewLogger("", "")))
}
