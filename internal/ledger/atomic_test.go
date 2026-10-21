package ledger

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"
)

func TestAtomicAwardAndReversalEntries(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	for _, e := range []Entry{
		{MemberID: "member", PaymentID: "first", Month: "2026-10", Points: 9500},
		{MemberID: "member", PaymentID: "partial", Month: "2026-10", Points: 1000},
		{MemberID: "member", PaymentID: "zero", Month: "2026-10", Points: 1000},
	} {
		if err := s.Award(ctx, e, 10000); err != nil {
			t.Fatal(err)
		}
	}
	for _, id := range []string{"partial", "zero", "partial", "zero"} {
		if found, err := s.Reverse(ctx, id); err != nil || !found {
			t.Fatalf("Reverse(%q) = %v, %v", id, found, err)
		}
	}
	if err := s.Award(ctx, Entry{MemberID: "other", PaymentID: "zero", Month: "2026-11", Points: 500}, 10000); err != nil {
		t.Fatal(err)
	}
	if err := s.Award(ctx, Entry{MemberID: "member", PaymentID: "after", Month: "2026-10", Points: 1000}, 10000); err != nil {
		t.Fatal(err)
	}
	want := []Entry{
		{MemberID: "member", PaymentID: "first", Kind: KindAward, Month: "2026-10", Points: 9500},
		{MemberID: "member", PaymentID: "partial", Kind: KindAward, Month: "2026-10", Points: 500},
		{MemberID: "member", PaymentID: "zero", Kind: KindAward, Month: "2026-10", Points: 0},
		{MemberID: "member", PaymentID: "partial", Kind: KindReversal, Month: "2026-10", Points: -500},
		{MemberID: "member", PaymentID: "zero", Kind: KindReversal, Month: "2026-10", Points: 0},
		{MemberID: "member", PaymentID: "after", Kind: KindAward, Month: "2026-10", Points: 0},
	}
	if !reflect.DeepEqual(s.entries, want) {
		t.Fatalf("entries = %+v, want %+v", s.entries, want)
	}
	if found, err := s.Reverse(ctx, "unknown"); err != nil || found {
		t.Fatalf("unknown reversal = %v, %v", found, err)
	}
	if len(s.entries) != len(want) {
		t.Fatal("unknown reversal wrote an entry")
	}
}

func TestAtomicOperationsFailWithoutWriting(t *testing.T) {
	for _, op := range []string{"award", "reverse"} {
		t.Run(op, func(t *testing.T) {
			fail := false
			s := NewMemoryStore(WithFaults(func(name string) error {
				if fail && name == op {
					return ErrUnavailable
				}
				return nil
			}))
			ctx := context.Background()
			e := Entry{MemberID: "member", PaymentID: "payment", Month: "2026-10", Points: 100}
			if op == "reverse" {
				if err := s.Award(ctx, e, 10000); err != nil {
					t.Fatal(err)
				}
			}
			before := append([]Entry(nil), s.entries...)
			fail = true
			var err error
			if op == "award" {
				err = s.Award(ctx, e, 10000)
			} else {
				_, err = s.Reverse(ctx, e.PaymentID)
			}
			if !errors.Is(err, ErrUnavailable) {
				t.Fatalf("error = %v, want unavailable", err)
			}
			if !reflect.DeepEqual(s.entries, before) {
				t.Fatal("failed operation changed the ledger")
			}
		})
	}
}

func TestAtomicOperationsHonorCanceledContext(t *testing.T) {
	s := NewMemoryStore()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := s.Award(ctx, Entry{PaymentID: "payment", Points: 100}, 10000); !errors.Is(err, context.Canceled) {
		t.Fatalf("award error = %v", err)
	}
	if _, err := s.Reverse(ctx, "payment"); !errors.Is(err, context.Canceled) {
		t.Fatalf("reverse error = %v", err)
	}
	if len(s.entries) != 0 {
		t.Fatal("canceled operation changed the ledger")
	}
}

func TestConcurrentReversalsWriteOneEntry(t *testing.T) {
	s := NewMemoryStore(WithLatency(time.Millisecond))
	ctx := context.Background()
	if err := s.Award(ctx, Entry{MemberID: "member", PaymentID: "payment", Month: "2026-10", Points: 500}, 10000); err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 30; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			if found, err := s.Reverse(ctx, "payment"); err != nil || !found {
				t.Errorf("reverse = %v, %v", found, err)
			}
		}()
	}
	close(start)
	wg.Wait()
	if len(s.entries) != 2 || s.entries[1].Points != -500 {
		t.Fatalf("entries = %+v", s.entries)
	}
}
