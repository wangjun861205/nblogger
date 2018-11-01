package nblogger

import (
	"log"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

type LogMessage struct {
	IP      string
	Message string
	Ok      bool
}

func TestLogger(t *testing.T) {
	logger, err := NewLogger("wangjun", "Wt20110523", "127.0.0.1:12345", "log")
	if err != nil {
		log.Fatal(err)
	}
	logger.Register(LogMessage{}, 5*time.Second)
	defer logger.ShutDown()
	for i := 0; i < 100; i++ {
		err := logger.Log(&LogMessage{"192.168.0.1", "oop test", false})
		if err != nil {
			log.Fatal(err)
		}
	}
}
