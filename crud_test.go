package dbcrud

import (
	"database/sql"
	"nomoninternet/dbcrud/dbunit"

	"testing"
)

func TestUpdateSuccess(t *testing.T) {

	var unit dbunit.UnitTest
	var db DB
	db.DB = unit.New(t)
	defer unit.ClearTable("testing")

	testData := make(map[string]interface{})
	testData["test_string"] = "Hello World"
	testData["test_int"] = 42
	testData["test_float"] = 1.23

	res, err := db.Insert("testing", testData)
	if err != nil {
		t.Errorf("Unable to insert data for testing update")
	}
	id, _ := res.LastInsertId()

	testData["test_string"] = "Hello World Update"
	testData["test_int"] = 43
	testData["test_float"] = 3.21

	wheres := make(map[string]interface{})
	wheres["id"] = id
	res, err = db.Update("testing", wheres, testData)
	if err != nil {
		t.Errorf("got error, wanted nil for error")
	}

	rows, _ := res.RowsAffected()
	if rows != 1 {
		t.Errorf("wanted 1 affected rows, got %v", rows)
	}

}

func TestInsertSuccess(t *testing.T) {

	var unit dbunit.UnitTest
	var db DB
	db.DB = unit.New(t)
	defer unit.ClearTable("testing")

	testData := make(map[string]interface{})
	testData["test_string"] = "Hello World"
	testData["test_int"] = 42
	testData["test_float"] = 1.23

	res, err := db.Insert("testing", testData)
	if err != nil {
		t.Errorf("got error, wanted nil for error")
	}

	rows, _ := res.RowsAffected()
	if rows != 1 {
		t.Errorf("wanted 1 affected rows, got %v", rows)
	}
}

func TestDeleteSuccess(t *testing.T) {

	var unit dbunit.UnitTest
	var db DB
	db.DB = unit.New(t)
	defer unit.ClearTable("testing")

	testData := make(map[string]interface{})
	testData["test_string"] = "Hello World"
	testData["test_int"] = 42
	testData["test_float"] = 1.23

	res, err := db.Insert("testing", testData)
	if err != nil {
		t.Errorf("Unable to insert data for testing update")
	}
	id, _ := res.LastInsertId()

	wheres := make(map[string]interface{})
	wheres["id"] = id
	res, err = db.Delete("testing", wheres)
	if err != nil {
		t.Errorf("got error, wanted nil for error")
	}

	rows, _ := res.RowsAffected()
	if rows != 1 {
		t.Errorf("wanted 1 affected rows, got %v", rows)
	}
}

func TestInsertTransactionSuccess(t *testing.T) {
	var unit dbunit.UnitTest
	var db *sql.DB
	db = unit.New(t)
	defer unit.ClearTable("testing")

	var tx Tx
	tx.Tx, _ = db.Begin()
	defer tx.Rollback()

	testData := make(map[string]interface{})
	testData["test_string"] = "Hello Transaction"
	testData["test_int"] = 42
	testData["test_float"] = 1.23

	res, err := tx.Insert("testing", testData)
	if err != nil {
		t.Errorf("got error, wanted nil for error")
	}

	rows, _ := res.RowsAffected()
	if rows != 1 {
		t.Errorf("wanted 1 affected rows, got %v", rows)
	}

	tx.Commit()
}

func TestPrepareInsertTransactionSuccess(t *testing.T) {
	testData := make(map[string]interface{})
	testData["test_string"] = "Hello Transaction"
	testData["test_int"] = 42
	testData["test_float"] = 1.23

	query, values := prepareInsertQuery("testing", testData)

	// The string is not exact as the fields may appear in random order
	// hence we only test for the length of the string
	want := "INSERT INTO `testing` (`test_string`, `test_int`, `test_float`) VALUES(?,?,?)"

	if len(query) != len(want) {
		t.Errorf("wanted: string of %v chracters got: %v", len(want), len(query))
	}

	// We only test if number of values match as values themselves can be in random order
	if len(values) != len(testData) {
		t.Errorf("wanted: %v values got: %v values", len(testData), len(values))
	}

}

func TestTransactionUpdateSuccess(t *testing.T) {

	var unit dbunit.UnitTest
	var db DB
	db.DB = unit.New(t)
	defer unit.ClearTable("testing")

	var tx Tx
	tx.Tx, _ = db.Begin()
	defer tx.Rollback()

	testData := make(map[string]interface{})
	testData["test_string"] = "Hello World"
	testData["test_int"] = 42
	testData["test_float"] = 1.23

	res, err := tx.Insert("testing", testData)
	if err != nil {
		t.Errorf("Unable to insert data for testing update")
	}
	id, _ := res.LastInsertId()

	testData["test_string"] = "Hello World Update"
	testData["test_int"] = 43
	testData["test_float"] = 3.21

	wheres := make(map[string]interface{})
	wheres["id"] = id
	res, err = tx.Update("testing", wheres, testData)
	if err != nil {
		t.Errorf("got error, wanted nil for error")
	}

	rows, _ := res.RowsAffected()
	if rows != 1 {
		t.Errorf("wanted 1 affected rows, got %v", rows)
	}

	tx.Commit()
}

func TestPrepareUpdateTransactionSuccess(t *testing.T) {
	testData := make(map[string]interface{})
	testData["test_string"] = "Hello Transaction"
	testData["test_int"] = 42
	testData["test_float"] = 1.23

	where := make(map[string]interface{})
	where["test_int"] = 42
	where["test_string"] = "Hello Transaction"

	query, values := prepareUpdateQuery("testing", where, testData)

	// The string is not exact as the fields may appear in random order
	// hence we only test for the length of the string
	want := "UPDATE `testing` SET  `test_string`=?, `test_int`=?, `test_float`=? WHERE  `test_int`=? AND  `test_string`=?"

	if len(query) != len(want) {
		t.Errorf("wanted: string of %v chracters got: %v", len(want), len(query))
	}

	// We only test if number of values match as values themselves can be in random order
	if len(values) != len(testData)+len(where) {
		t.Errorf("wanted: %v values got: %v values", len(testData)+len(where), len(values))
	}

}

func TestPrepareDeleteSuccess(t *testing.T) {

	where := make(map[string]interface{})
	where["test_int"] = 42
	where["test_string"] = "Hello Transaction"

	query, values := prepareDeleteQuery("testing", where)

	// The string is not exact as the fields may appear in random order
	// hence we only test for the length of the string
	want := "DELETE FROM `testing` WHERE  `test_string`=? AND  `test_int`=?"

	if len(query) != len(want) {
		t.Errorf("wanted: string of %v chracters got: %v", len(want), len(query))
	}

	// We only test if number of values match as values themselves can be in random order
	if len(values) != len(where) {
		t.Errorf("wanted: %v values got: %v values", len(where), len(values))
	}
}

func TestTransactionDeleteSuccess(t *testing.T) {

	var unit dbunit.UnitTest
	var db DB
	db.DB = unit.New(t)
	defer unit.ClearTable("testing")

	var tx Tx
	tx.Tx, _ = db.Begin()
	defer tx.Rollback()

	testData := make(map[string]interface{})
	testData["test_string"] = "Hello World"
	testData["test_int"] = 42
	testData["test_float"] = 1.23

	res, err := tx.Insert("testing", testData)
	if err != nil {
		t.Errorf("Unable to insert data for testing update")
	}
	id, _ := res.LastInsertId()

	wheres := make(map[string]interface{})
	wheres["id"] = id
	res, err = tx.Delete("testing", wheres)
	if err != nil {
		t.Errorf("got error, wanted nil for error")
	}

	rows, _ := res.RowsAffected()
	if rows != 1 {
		t.Errorf("wanted 1 affected rows, got %v", rows)
	}
}
