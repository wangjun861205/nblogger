package nblogger

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/wangjun861205/nborm"
)

type Logger struct {
	db          *sql.DB
	fieldMap    map[string][]fieldInfo
	fieldLock   sync.RWMutex
	stmtMap     map[string]*sql.Stmt
	stmtLock    sync.RWMutex
	watchCtx    context.Context
	cancelWatch context.CancelFunc
	watcherWG   sync.WaitGroup
}

func NewLogger(username, password, address, database string) (*Logger, error) {
	db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s)/%s", username, password, address, database))
	if err != nil {
		return nil, err
	}
	nborm.RegisterDB(username, password, address, "information_schema")
	ctx, cancel := context.WithCancel(context.Background())
	return &Logger{
		db:          db,
		fieldMap:    make(map[string][]fieldInfo),
		stmtMap:     make(map[string]*sql.Stmt),
		watchCtx:    ctx,
		cancelWatch: cancel,
	}, nil
}

func (logger *Logger) Register(obj interface{}, keepTime time.Duration) error {
	typ := reflect.TypeOf(obj)
	for typ.Kind() == reflect.Ptr || typ.Kind() == reflect.Interface {
		typ = typ.Elem()
	}
	if typ.Kind() != reflect.Struct {
		return fmt.Errorf("nborm.Register() error: only struct can be registered")
	}
	exist, err := logger.tableExists(typ)
	if err != nil {
		return err
	}
	if exist {
		if err := checkTable(typ); err != nil {
			return err
		}
	} else {
		err := logger.createTable(typ)
		if err != nil {
			return err
		}
	}
	fieldList := make([]fieldInfo, typ.NumField())
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		fieldList[i].name = field.Name
		fieldList[i].typ = field.Type.String()
		fieldList[i].offset = field.Offset
	}
	logger.fieldLock.Lock()
	logger.fieldMap[typ.Name()] = fieldList
	logger.fieldLock.Unlock()
	logger.watcherWG.Add(1)
	go func() {
		ticker := time.NewTicker(keepTime)
		for {
			select {
			case <-logger.watchCtx.Done():
				ticker.Stop()
				logger.watcherWG.Done()
				return
			case now := <-ticker.C:
				_, err := logger.db.Exec(fmt.Sprintf("DELETE FROM %s WHERE log_time < ?", typ.Name()),
					now.Truncate(keepTime).Format("2006-01-02 15:04:05"))
				if err != nil {
					fmt.Println(err.Error())
					ticker.Stop()
					logger.watcherWG.Done()
					return
				}
			}
		}
	}()
	return nil
}

func (logger *Logger) Log(obj interface{}) error {
	typ := reflect.TypeOf(obj)
	for typ.Kind() == reflect.Ptr || typ.Kind() == reflect.Interface {
		typ = typ.Elem()
	}
	logger.fieldLock.RLock()
	fieldList, exists := logger.fieldMap[typ.Name()]
	if !exists {
		return fmt.Errorf("nblogger.Log() error: register struct before log")
	}
	val := reflect.ValueOf(obj)
	start := val.Pointer()
	colList := make([]string, typ.NumField())
	valList := make([]interface{}, typ.NumField())
	for i, field := range fieldList {
		colList[i] = field.name
		p := unsafe.Pointer(start + field.offset)
		switch field.typ {
		case "string":
			v := (*string)(p)
			valList[i] = *v
		case "int":
			v := (*int)(p)
			valList[i] = *v
		case "int8":
			v := (*int8)(p)
			valList[i] = *v
		case "int16":
			v := (*int16)(p)
			valList[i] = *v
		case "int32":
			v := (*int32)(p)
			valList[i] = *v
		case "int64":
			v := (*int64)(p)
			valList[i] = *v
		case "uint":
			v := (*uint)(p)
			valList[i] = *v
		case "uint8":
			v := (*uint8)(p)
			valList[i] = *v
		case "uint16":
			v := (*uint16)(p)
			valList[i] = *v
		case "uint32":
			v := (*uint32)(p)
			valList[i] = *v
		case "uint64":
			v := (*uint64)(p)
			valList[i] = *v
		case "float32":
			v := (*float32)(p)
			valList[i] = *v
		case "float64":
			v := (*float64)(p)
			valList[i] = *v
		case "bool":
			v := (*bool)(p)
			valList[i] = *v
		case "time.Time":
			v := (*time.Time)(p)
			valList[i] = v.Format("2006-01-02 15:04:05")
		default:
			return fmt.Errorf("nborm.Log() error: not supported type (%s)", field.typ)
		}
	}
	var stmt *sql.Stmt
	logger.stmtLock.RLock()
	var ok bool
	stmt, ok = logger.stmtMap[typ.Name()]
	if !ok {
		var err error
		stmt, err = logger.db.Prepare(fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", typ.Name(), strings.Join(colList, ", "),
			strings.Join(strings.Fields(strings.Repeat("? ", len(fieldList))), ", ")))
		if err != nil {
			return err
		}
		logger.stmtLock.RUnlock()
		logger.stmtLock.Lock()
		logger.stmtMap[typ.Name()] = stmt
		logger.stmtLock.Unlock()
		_, err = stmt.Exec(valList...)
		return err
	}
	logger.stmtLock.RUnlock()
	_, err := stmt.Exec(valList...)
	return err
}

func (logger *Logger) ShutDown() error {
	logger.stmtLock.Lock()
	defer logger.stmtLock.Unlock()
	for _, stmt := range logger.stmtMap {
		err := stmt.Close()
		if err != nil {
			return err
		}
	}
	logger.cancelWatch()
	logger.watcherWG.Wait()
	err := nborm.CloseConns()
	if err != nil {
		return err
	}
	return logger.db.Close()

}

func (logger *Logger) tableExists(typ reflect.Type) (bool, error) {
	var n int
	row := logger.db.QueryRow("SELECT 1 FROM information_schema.tables WHERE table_name = ?", typ.Name())
	err := row.Scan(&n)
	if err != nil {
		if err == sql.ErrNoRows {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (logger *Logger) createTable(typ reflect.Type) error {
	pk := "id"
	builder := strings.Builder{}
	builder.WriteString(fmt.Sprintf("CREATE TABLE %s ( id INT NOT NULL AUTO_INCREMENT, ", typ.Name()))
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		if _, exist := field.Tag.Lookup("primary_key"); exist {
			pk = field.Name
		}
		colType, exist := goToSQLType[field.Type.String()]
		if !exist {
			return fmt.Errorf("nblogger.createTable() error: not supported type (%s)", field.Type.String())
		}
		builder.WriteString(fmt.Sprintf("%s %s NOT NULL, ", field.Name, colType))
	}
	builder.WriteString(fmt.Sprintf("log_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY (%s))", pk))
	_, err := logger.db.Exec(builder.String())
	return err
}
