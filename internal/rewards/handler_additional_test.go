package rewards_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rentpoints/pointsd/internal/ledger"
	"github.com/rentpoints/pointsd/internal/rewards"
)

func TestReversalConcurrentAcrossThreeReplicas(t *testing.T) {
	s := ledger.NewMemoryStore(ledger.WithLatency(dbLatency))
	replicas := []*rewards.Handler{newHandler(s), newHandler(s), newHandler(s)}
	deliver(t, replicas[0], settled("first", "member", 9500))
	deliver(t, replicas[0], settled("partial", "member", 1000))
	ev := reversed("partial", "wrong_member", 8000)
	ev.OccurredAt = oct.AddDate(0, 1, 0)
	var fns []func()
	for i := 0; i < 30; i++ {
		h := replicas[i%len(replicas)]
		fns = append(fns, func() { deliver(t, h, ev) })
	}
	concurrently(fns...)
	deliver(t, replicas[1], settled("after", "member", 1000))
	if got := balance(t, s, "member"); got != 9500 {
		t.Fatalf("balance = %d, want 9500", got)
	}
	if got := balance(t, s, "wrong_member"); got != 0 {
		t.Fatalf("other member balance = %d, want 0", got)
	}
	if got, err := s.AwardedInMonth(context.Background(), "member", "2026-10"); err != nil || got != 10000 {
		t.Fatalf("monthly awards = %d, %v, want 10000", got, err)
	}
}

func TestRepublishedZeroAwardDoesNotEarnInAnotherMonth(t *testing.T) {
	s := ledger.NewMemoryStore()
	h := newHandler(s)
	deliver(t, h, settled("full", "member", 10000))
	ev := settled("zero", "member", 1000)
	deliver(t, h, ev)
	ev.EventID = "republished"
	ev.OccurredAt = oct.AddDate(0, 1, 0)
	deliver(t, h, ev)
	deliver(t, h, reversed("zero", "member", 1000))
	if got := balance(t, s, "member"); got != 10000 {
		t.Fatalf("balance = %d, want 10000", got)
	}
	if seen, err := s.HasEntry(context.Background(), "zero", ledger.KindReversal); err != nil || !seen {
		t.Fatalf("zero award reversal = %v, %v", seen, err)
	}
}

func TestHandleLedgerFailureNacksThenRedeliverySucceeds(t *testing.T) {
	for _, op := range []string{"award", "reverse"} {
		t.Run(op, func(t *testing.T) {
			var fail atomic.Bool
			s := ledger.NewMemoryStore(ledger.WithFaults(func(name string) error {
				if name == op && fail.CompareAndSwap(true, false) {
					return ledger.ErrUnavailable
				}
				return nil
			}))
			h := newHandler(s)
			ev := settled("payment", "member", 1000)
			wantBefore, wantAfter := int64(0), int64(1000)
			if op == "reverse" {
				deliver(t, h, ev)
				ev = reversed("payment", "member", 1000)
				wantBefore, wantAfter = 1000, 0
			}
			fail.Store(true)
			msg := newMessage(t, ev)
			h.Handle(context.Background(), msg)
			if msg.outcome(t) != nacked {
				t.Fatal("ledger failure was not nacked")
			}
			if got := balance(t, s, "member"); got != wantBefore {
				t.Fatalf("balance after failure = %d, want %d", got, wantBefore)
			}
			deliver(t, newHandler(s), ev)
			deliver(t, h, ev)
			if got := balance(t, s, "member"); got != wantAfter {
				t.Fatalf("balance after redelivery = %d, want %d", got, wantAfter)
			}
		})
	}
}

type failingPublisher struct{}

func (failingPublisher) Publish(context.Context, []byte, map[string]string) error {
	return errors.New("publisher unavailable")
}

func TestHandleDeadLetterFailureNacks(t *testing.T) {
	h := rewards.NewHandler(ledger.NewMemoryStore(), rewards.DefaultPolicy, failingPublisher{}, newHandlerLogger())
	msg := &fakeMessage{id: "invalid", data: []byte(`{"type":"invalid"}`)}
	h.Handle(context.Background(), msg)
	if msg.outcome(t) != nacked {
		t.Fatal("dead-letter failure was not nacked")
	}
}

func TestAwardTruncatesCentsAndUsesUTCMonth(t *testing.T) {
	s := ledger.NewMemoryStore()
	h := newHandler(s)
	ev := settled("payment", "member", 1)
	ev.AmountCents = 199
	ev.OccurredAt = time.Date(2026, 11, 1, 0, 30, 0, 0, time.FixedZone("UTC+2", 2*60*60))
	deliver(t, h, ev)
	if got := balance(t, s, "member"); got != 1 {
		t.Fatalf("balance = %d, want 1", got)
	}
	if got, err := s.AwardedInMonth(context.Background(), "member", "2026-10"); err != nil || got != 1 {
		t.Fatalf("UTC month awards = %d, %v, want 1", got, err)
	}
	ev.PaymentID = "fraction"
	ev.AmountCents = 99
	deliver(t, h, ev)
	if seen, err := s.HasEntry(context.Background(), "fraction", ledger.KindAward); err != nil || !seen {
		t.Fatalf("fractional award recorded = %v, %v", seen, err)
	}
}
