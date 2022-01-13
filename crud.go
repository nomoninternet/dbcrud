package dbcrud

import (
	"database/sql"
	"fmt"
	"strings"
)

type DB struct {
	*sql.DB
}

type Tx struct {
	*sql.Tx
}

func prepareInsertQuery(table string, fields map[string]interface{}) (query string, values []interface{}) {

	var sets, placeholders []string

	query = "INSERT INTO `" + table + "` ("
	for k, v := range fields {
		sets = append(sets, "`"+k+"`")
		placeholders = append(placeholders, "?")
		values = append(values, v)
	}

	query += strings.Join(sets, ", ") + ") VALUES(" + strings.Join(placeholders, ",") + ")"

	return query, values
}

// Insert a new row into given table with data supplied in fields as part of a transaction. The index of fields map
// must correspond to table column names.
func (tx *Tx) Insert(table string, fields map[string]interface{}) (res sql.Result, err error) {

	query, values := prepareInsertQuery(table, fields)

	res, err = tx.Exec(query, values...)
	if err != nil {
		err = fmt.Errorf("Unable to insert into table %s * %w", table, err)
		return res, err
	}

	return res, nil
}

// Insert a new row into given table with data supplied in fields. The index of fields map
// must correspond to table column names.
func (db *DB) Insert(table string, fields map[string]interface{}) (res sql.Result, err error) {

	query, values := prepareInsertQuery(table, fields)

	res, err = db.Exec(query, values...)
	if err != nil {
		err = fmt.Errorf("Unable to insert into table %s * %w", table, err)
		return res, err
	}

	return res, nil
}

func prepareUpdateQuery(table string, where map[string]interface{}, fields map[string]interface{}) (query string, values []interface{}) {
	var sets []string
	//var values []interface{}

	query = "UPDATE `" + table + "` SET "
	for k, v := range fields {
		sets = append(sets, " `"+k+"`=?")
		values = append(values, v)
	}

	var wheres []string
	for k, v := range where {
		wheres = append(wheres, " `"+k+"`=?")
		values = append(values, v)
	}

	query += strings.Join(sets, ",") + " WHERE " + strings.Join(wheres, " AND ")

	return query, values
}

// Update a row in the given table identified by where parameters with key-value pairs supplied for fields.
// The index of the maps for where and fields identifies the field by name.
// If map contains multiple entries, all must match (AND operator).
func (db *DB) Update(table string, where map[string]interface{}, fields map[string]interface{}) (res sql.Result, err error) {

	query, values := prepareUpdateQuery(table, where, fields)
	res, err = db.Exec(query, values...)
	if err != nil {
		err = fmt.Errorf("Unable to update table %s * %w", table, err)
		return res, err
	}

	return res, nil
}

func (tx *Tx) Update(table string, where map[string]interface{}, fields map[string]interface{}) (res sql.Result, err error) {

	query, values := prepareUpdateQuery(table, where, fields)
	res, err = tx.Exec(query, values...)
	if err != nil {
		err = fmt.Errorf("Unable to update table %s * %w", table, err)
		return res, err
	}

	return res, nil
}

func prepareDeleteQuery(table string, where map[string]interface{}) (query string, values []interface{}) {

	query = "DELETE FROM `" + table + "` WHERE "
	var wheres []string
	for k, v := range where {
		wheres = append(wheres, " `"+k+"`=?")
		values = append(values, v)
	}

	query += strings.Join(wheres, " AND ")

	return query, values
}

// Delete rows from given table where rows are identified by where map.
// Index of where refers to table names. If map contains multiple entries, all must match (AND operator).
func (db *DB) Delete(table string, where map[string]interface{}) (res sql.Result, err error) {

	query, values := prepareDeleteQuery(table, where)
	res, err = db.Exec(query, values...)
	if err != nil {
		err = fmt.Errorf("Unable to delete from table %s * %w", table, err)
		return res, err
	}

	return res, nil
}

func (tx *Tx) Delete(table string, where map[string]interface{}) (res sql.Result, err error) {

	query, values := prepareDeleteQuery(table, where)
	res, err = tx.Exec(query, values...)
	if err != nil {
		err = fmt.Errorf("Unable to delete from table %s * %w", table, err)
		return res, err
	}

	return res, nil
}
