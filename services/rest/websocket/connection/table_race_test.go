package connection

import (
	"github.com/goto/raccoon/identification"
	"testing"
	"time"
)

func TestRaceInTable(t *testing.T) {
	table := NewTable(100)
	done := make(chan bool)
	id := identification.Identifier{
		ID:    "connection-id-1",
		Group: "group-i-1",
	}

	//store the ids
	go func() {
		for {
			select {
			case <-done:
				return
			default:
				table.Store(id)
			}
		}
	}()

	go func() {
		for {
			select {
			case <-done:
				return
			default:
				for k, v := range table.TotalConnectionPerGroup() {
					_ = k
					_ = v
				}
			}
		}
	}()

	time.Sleep(2 * time.Second)
	close(done)
}
