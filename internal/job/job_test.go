package job

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestEveryStateRoundTripsThroughJSON(t *testing.T) {
	for _, st := range []State{0, StateReady, StateScheduled, StateInFlight, StateDead, StateComplete} {
		j := Job{ID: "x", Queue: "q", Type: "t", State: st, Payload: json.RawMessage(`{"a":1}`), CreatedAt: time.Unix(1, 0).UTC()}
		b, err := json.Marshal(j)
		if err != nil {
			t.Fatal(err)
		}
		var back Job
		if err := json.Unmarshal(b, &back); err != nil {
			t.Fatalf("state %v: %v (%s)", st, err, b)
		}
		if back.State != st {
			t.Errorf("state %v came back as %v", st, back.State)
		}
	}
	var s State
	if err := s.UnmarshalText([]byte("bogus")); err == nil {
		t.Error("unknown state names must still be rejected")
	}
}

func TestNewIDIsUniqueAndTimeSortable(t *testing.T) {
	seen := map[string]bool{}
	var prev string
	for i := 0; i < 1000; i++ {
		id := NewID()
		if len(id) != 32 || seen[id] {
			t.Fatalf("bad or duplicate id %q", id)
		}
		seen[id] = true
		if i%100 == 0 {
			if prev != "" && strings.Compare(id[:16], prev[:16]) < 0 {
				t.Fatal("timestamp prefix went backwards")
			}
			prev = id
			time.Sleep(2 * time.Millisecond)
		}
	}
}
