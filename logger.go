package nblogger

import (
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

type logFileList []string

func (l logFileList) Less(i, j int) bool {
	iName := strings.Replace(strings.Replace(l[i], "_", "", -1), ".log", "", -1)
	jName := strings.Replace(strings.Replace(l[j], "_", "", -1), ".log", "", -1)
	iNum, _ := strconv.ParseInt(iName, 10, 64)
	jNum, _ := strconv.ParseInt(jName, 10, 64)
	return iNum < jNum
}

func (l logFileList) Len() int {
	return len(l)
}

func (l logFileList) Swap(i, j int) {
	l[i], l[j] = l[j], l[i]
}

type Logger interface {
	Log(interface{})
	Stop()
}

type IntervalLogger struct {
	inputChan    chan interface{}
	stopChan     chan interface{}
	intervalChan <-chan time.Time
	path         string
	logger       *log.Logger
	file         *os.File
	keepNum      int
}

func NewIntervalLogger(path string, interval time.Duration, format int, prefix string, keepNum int) (*IntervalLogger, error) {
	err := mkdirIfNotExists(path)
	if err != nil {
		return nil, err
	}
	f, err := newLogFile(path)
	if err != nil {
		return nil, err
	}
	inputChan := make(chan interface{})
	stopChan := make(chan interface{})
	logger := log.New(f, prefix, format)
	intervalChan := time.Tick(interval)
	intervalLogger := &IntervalLogger{inputChan, stopChan, intervalChan, path, logger, f, keepNum}
	intervalLogger.start()
	return intervalLogger, nil
}

func (logger *IntervalLogger) start() {
	go func() {
		for {
			select {
			case <-logger.intervalChan:
				logger.file.Close()
				newFile, err := newLogFile(logger.path)
				if err != nil {
					return
				}
				logger.logger.SetOutput(newFile)
				logger.file = newFile
				cleanLogFiles(logger.path, logger.keepNum)
			case msg := <-logger.inputChan:
				logger.logger.Println(msg)
			case <-logger.stopChan:
				logger.file.Close()
				close(logger.inputChan)
				return
			}
		}
	}()
}

func (logger *IntervalLogger) Log(msg interface{}) {
	logger.inputChan <- msg
}

func (logger *IntervalLogger) Stop() {
	close(logger.stopChan)
}

type FixedSizeLogger struct {
	path         string
	keepNum      int
	inputChan    chan interface{}
	stopChan     chan interface{}
	intervalChan <-chan time.Time
	logger       *log.Logger
	file         *os.File
	sizeLimit    int
}

func NewFixedSizeLogger(path, prefix string, format int, keepNum int, interval time.Duration, sizeLimit int) (*FixedSizeLogger, error) {
	err := mkdirIfNotExists(path)
	if err != nil {
		return nil, err
	}
	f, err := newLogFile(path)
	if err != nil {
		return nil, err
	}
	inputChan := make(chan interface{})
	stopChan := make(chan interface{})
	intervalChan := time.Tick(interval)
	logger := log.New(f, prefix, format)
	fixedLogger := &FixedSizeLogger{path, keepNum, inputChan, stopChan, intervalChan, logger, f, sizeLimit}
	fixedLogger.start()
	return fixedLogger, nil
}

func (logger *FixedSizeLogger) start() {
	go func() {
		for {
			select {
			case <-logger.intervalChan:
				info, _ := logger.file.Stat()
				size := info.Size()
				if size > int64(logger.sizeLimit) {
					logger.file.Close()
					newFile, _ := newLogFile(logger.path)
					logger.logger.SetOutput(newFile)
					logger.file = newFile
					cleanLogFiles(logger.path, logger.keepNum)
				}
			case msg := <-logger.inputChan:
				logger.logger.Println(msg)
			case <-logger.stopChan:
				logger.file.Close()
				close(logger.inputChan)
				return
			}
		}
	}()
}

func (logger *FixedSizeLogger) Stop() {
	close(logger.stopChan)
}

func (logger *FixedSizeLogger) Log(msg interface{}) {
	logger.inputChan <- msg
}

func newLogFile(path string) (*os.File, error) {
	filename := time.Now().Format("2006_01_02_15_04_05") + ".log"
	return os.OpenFile(filepath.Join(path, filename), os.O_CREATE|os.O_WRONLY, 0755)
}

func cleanLogFiles(path string, keepNum int) error {
	infos, err := ioutil.ReadDir(path)
	if err != nil {
		return err
	}
	logFiles := make(logFileList, 0, len(infos))
	for _, info := range infos {
		if !info.IsDir() && info.Name()[len(info.Name())-4:] == ".log" {
			logFiles = append(logFiles, info.Name())
		}
	}
	if len(logFiles) > keepNum {
		sort.Sort(logFiles)
		for _, filename := range logFiles[:len(logFiles)-keepNum] {
			err := os.Remove(filepath.Join(path, filename))
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func mkdirIfNotExists(path string) error {
	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			err = os.MkdirAll(path, 0775)
			if err != nil {
				return err
			}
		}
		return err
	}
	if !info.IsDir() {
		err = os.MkdirAll(path, 0775)
		if err != nil {
			return err
		}
	}
	return nil
}

func NewLogger(path, prefix string, format int, interval time.Duration, sizeLimit int, keepNum int) (Logger, error) {
	if sizeLimit > 0 {
		return NewFixedSizeLogger(path, prefix, format, keepNum, interval, sizeLimit)
	}
	return NewIntervalLogger(path, interval, format, prefix, keepNum)
}
