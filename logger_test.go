package nblogger

import (
	"log"
	"testing"
	"time"
)

func TestLogger(t *testing.T) {
	logger, err := NewLogger("./", "notbear:", log.Ltime|log.Lshortfile, 1*time.Second, 256, 3)
	if err != nil {
		panic(err)
	}
	defer logger.Stop()
	for i := 0; i < 100; i++ {
		logger.Log("hello not bear")
		time.Sleep(100 * time.Millisecond)
	}
}
