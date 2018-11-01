package nblogger

import (
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/wangjun861205/nborm"
)

type columnInfo struct {
	tableCatalog           *nborm.StringField
	tableSchema            *nborm.StringField
	tableName              *nborm.StringField
	columnName             *nborm.StringField
	ordinalPosition        *nborm.IntField
	columnDefault          *nborm.StringField
	isNullable             *nborm.StringField
	dataType               *nborm.StringField
	characterMaximumLength *nborm.IntField
	characterOctetLength   *nborm.IntField
	numericPrecision       *nborm.IntField
	numericScale           *nborm.IntField
	datetimePrecision      *nborm.IntField
	characterSetName       *nborm.StringField
	collationName          *nborm.StringField
	columnType             *nborm.StringField
	columnKey              *nborm.StringField
	extra                  *nborm.StringField
	privileges             *nborm.StringField
	columnComment          *nborm.StringField
	generationExpression   *nborm.StringField
	srsID                  *nborm.IntField
	_isSync                bool
}

//NewColumn create Column
func newColumnInfo() *columnInfo {
	col := &columnInfo{}
	col.tableCatalog = nborm.NewStringField(col, "TABLE_CATALOG", false, false, false)
	col.tableSchema = nborm.NewStringField(col, "TABLE_SCHEMA", false, false, false)
	col.tableName = nborm.NewStringField(col, "TABLE_NAME", false, false, false)
	col.columnName = nborm.NewStringField(col, "COLUMN_NAME", false, false, false)
	col.ordinalPosition = nborm.NewIntField(col, "ORDINAL_POSITION", false, false, false)
	col.columnDefault = nborm.NewStringField(col, "COLUMN_DEFAULT", false, false, false)
	col.isNullable = nborm.NewStringField(col, "IS_NULLABLE", false, false, false)
	col.dataType = nborm.NewStringField(col, "DATA_TYPE", false, false, false)
	col.characterMaximumLength = nborm.NewIntField(col, "CHARACTER_MAXIMUM_LENGTH", false, false, false)
	col.characterOctetLength = nborm.NewIntField(col, "CHARACTER_OCTET_LENGTH", false, false, false)
	col.numericPrecision = nborm.NewIntField(col, "NUMERIC_PRECISION", false, false, false)
	col.numericScale = nborm.NewIntField(col, "NUMERIC_SCALE", false, false, false)
	col.datetimePrecision = nborm.NewIntField(col, "DATETIME_PRECISION", false, false, false)
	col.characterSetName = nborm.NewStringField(col, "CHARACTER_SET_NAME", false, false, false)
	col.collationName = nborm.NewStringField(col, "COLLATION_NAME", false, false, false)
	col.columnType = nborm.NewStringField(col, "COLUMN_TYPE", false, false, false)
	col.columnKey = nborm.NewStringField(col, "COLUMN_KEY", false, false, false)
	col.extra = nborm.NewStringField(col, "EXTRA", false, false, false)
	col.privileges = nborm.NewStringField(col, "PRIVILEGES", false, false, false)
	col.columnComment = nborm.NewStringField(col, "COLUMN_COMMENT", false, false, false)
	col.generationExpression = nborm.NewStringField(col, "GENERATION_EXPRESSION", false, false, false)
	col.srsID = nborm.NewIntField(col, "SRS_ID", false, false, false)
	return col
}

//DB database name
func (c *columnInfo) DB() string {
	return "information_schema"
}

//Tab table name
func (c *columnInfo) Tab() string {
	return "columns"
}

//Fields all data Field
func (c *columnInfo) Fields() []nborm.Field {
	return []nborm.Field{
		c.tableCatalog,
		c.tableSchema,
		c.tableName,
		c.columnName,
		c.ordinalPosition,
		c.columnDefault,
		c.isNullable,
		c.dataType,
		c.characterMaximumLength,
		c.characterOctetLength,
		c.numericPrecision,
		c.numericScale,
		c.datetimePrecision,
		c.characterSetName,
		c.collationName,
		c.columnType,
		c.columnKey,
		c.extra,
		c.privileges,
		c.columnComment,
		c.generationExpression,
		c.srsID,
	}
}

//SetSync set synchronized status
func (c *columnInfo) SetSync(b bool) {
	c._isSync = b
}

//GetSync get synchronized status
func (c *columnInfo) GetSync() bool {
	return c._isSync
}

//ColumnList Column list
type columnList struct {
	m    *columnInfo
	list []*columnInfo
}

//NewColumnList create ColumnList
func newColumnList() *columnList {
	return &columnList{
		newColumnInfo(),
		make([]*columnInfo, 0, 128),
	}
}

//New create Column and append it to list
func (cl *columnList) New() nborm.Model {
	c := newColumnInfo()
	cl.list = append(cl.list, c)
	return c
}

//Len length of list
func (cl *columnList) Len() int {
	return len(cl.list)
}

//Index index operation
func (cl *columnList) Index(i int) nborm.Model {
	return (cl.list)[i]
}

//Delete delete operation
func (cl *columnList) Delete(i int) {
	switch i {
	case 0:
		cl.list = (cl.list)[1:]
	case cl.Len() - 1:
		cl.list = (cl.list)[:cl.Len()-1]
	default:
		cl.list = append((cl.list)[:i], (cl.list)[i+1:]...)
	}
}

//Swap swap element
func (cl *columnList) Swap(i, j int) {
	(cl.list)[i], (cl.list)[j] = (cl.list)[j], (cl.list)[i]
}

//MarshalJSON implement json.Marshaler interface
func (cl *columnList) MarshalJSON() ([]byte, error) {
	return json.MarshalIndent(cl.list, "\t", "\t")
}

//Model return example Model
func (cl *columnList) Model() nborm.Model {
	return cl.m
}

var goToSQLType = map[string]string{
	"string":    "varchar(512)",
	"int":       "bigint(20)",
	"int8":      "tinyint(4)",
	"int16":     "smallint(6)",
	"int32":     "int(11)",
	"int64":     "bigint(20)",
	"uint":      "int(20) unsigned",
	"uint8":     "tinyint(4) unsigned",
	"uint16":    "smallint(6) unsigned",
	"uint32":    "int(11) unsigned",
	"uint64":    "bigint(20) unsigned",
	"float32":   "decimal(32, 4)",
	"float64":   "decimal(65, 4)",
	"bool":      "tinyint(1)",
	"time.Time": "datetime",
}

func checkTable(typ reflect.Type) error {
	colList := newColumnList()
	err := nborm.Query(colList, colList.m.tableName.Eq(typ.Name()), nborm.NewSorter(nborm.NewOrder(colList.m.ordinalPosition, false)), nil)
	if err != nil {
		return err
	}
	if colList.Len()-2 != typ.NumField() {
		return fmt.Errorf("nblogger.checkTable() error: column number not match (field number: %d, column number: %d - 2", typ.NumField(), colList.Len())
	}
	for i, col := range colList.list[1 : colList.Len()-1] {
		field := typ.Field(i)
		colName, _, _ := col.columnName.Get()
		if field.Name != colName {
			return fmt.Errorf("nblogger.checkTable() error: column name not match (field name: %s, column name: %s)", field.Name, colName)
		}
		if _, exist := field.Tag.Lookup("primary_key"); exist {
			key, _, null := col.columnKey.Get()
			if null || key != "PRI" {
				return fmt.Errorf("nblogger.checkTable() error: primary key not match (%s)", field.Name)
			}
		}
		colType, _, _ := col.columnType.Get()
		if goToSQLType[field.Type.String()] != colType {
			return fmt.Errorf("nblogger.checkTable() error: type not match (field type: %s, column type: %s)", goToSQLType[field.Type.String()],
				colType)
		}
	}
	return nil
}

type fieldInfo struct {
	name   string
	typ    string
	offset uintptr
}
