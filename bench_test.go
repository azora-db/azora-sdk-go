package azora_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	azora "github.com/azora-db/azora-go"
)

// Benchmarks against the real engine, client and server on one machine,
// over the encrypted TCP channel:
//
//	go test -run '^$' -bench . -benchtime 2s
//
// They share the test engine, whose per-connection request rate is
// lifted (the default, 1000 requests a second, would be what is measured
// otherwise).

type benchRow struct {
	ID         int64
	CustomerID int64 `db:"customer_id"`
	Status     string
	Total      float64
}

// benchDB connects and fills t_bench with n rows.
func benchDB(b *testing.B, n int, opts ...azora.Option) *azora.DB {
	b.Helper()
	db := engine(b).connect(b, opts...)
	bg := context.Background()
	_, err := db.Exec(bg, "DELETE FROM t_bench")
	require.NoError(b, err)
	for start := 0; start < n; start += 500 {
		batch := db.NewBatch()
		for i := start; i < min(n, start+500); i++ {
			batch.Add("INSERT INTO t_bench(id, customer_id, status, total) VALUES (?, ?, ?, ?)", i+1, i%100, "open", float64(i)*1.5)
		}
		_, err := batch.Run(bg)
		require.NoError(b, err)
	}
	b.Cleanup(func() { db.Exec(context.Background(), "DELETE FROM t_bench") })
	return db
}

// One request at a time on one connection: the round-trip cost.
func BenchmarkPointRead(b *testing.B) {
	db := benchDB(b, 1000)
	bg := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		row, err := azora.One[benchRow](bg, db, "SELECT id, customer_id, status, total FROM t_bench WHERE id = ?", i%1000+1)
		if err != nil || row.ID == 0 {
			b.Fatal(err)
		}
	}
}

// Many goroutines sharing one connection: requests are multiplexed.
func BenchmarkPointReadParallel(b *testing.B) {
	db := benchDB(b, 1000)
	benchParallelReads(b, db)
}

// The same over a pool of four connections.
func BenchmarkPointReadParallelPool4(b *testing.B) {
	db := benchDB(b, 1000, azora.WithPool(4))
	benchParallelReads(b, db)
}

func benchParallelReads(b *testing.B, db *azora.DB) {
	bg := context.Background()
	var n atomic.Int64
	b.SetParallelism(8)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			id := n.Add(1)%1000 + 1
			if _, err := azora.Scalar[float64](bg, db, "SELECT total FROM t_bench WHERE id = ?", id); err != nil {
				b.Error(err)
				return
			}
		}
	})
}

// A result of 1,000 rows decoded into structs.
func BenchmarkSelect1000Rows(b *testing.B) {
	db := benchDB(b, 1000)
	bg := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows, err := azora.All[benchRow](bg, db, "SELECT id, customer_id, status, total FROM t_bench")
		if err != nil || len(rows) != 1000 {
			b.Fatal(err, len(rows))
		}
	}
}

// One insert per request, each an exactly-once write.
func BenchmarkInsert(b *testing.B) {
	db := benchDB(b, 0)
	bg := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := db.Exec(bg, "INSERT INTO t_bench(customer_id, status, total) VALUES (?, 'open', ?)", i%100, 9.5); err != nil {
			b.Fatal(err)
		}
	}
}

// Inserts from many goroutines over one connection.
func BenchmarkInsertParallel(b *testing.B) {
	db := benchDB(b, 0)
	bg := context.Background()
	b.SetParallelism(8)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := db.Exec(bg, "INSERT INTO t_bench(customer_id, status, total) VALUES (1, 'open', 9.5)"); err != nil {
				b.Error(err)
				return
			}
		}
	})
}

// Ten inserts in one all-or-nothing batch; ns/op is per batch.
func BenchmarkBatch10(b *testing.B) {
	db := benchDB(b, 0)
	bg := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		batch := db.NewBatch()
		for j := 0; j < 10; j++ {
			batch.Add("INSERT INTO t_bench(customer_id, status, total) VALUES (?, 'open', ?)", j, 1.25)
		}
		if _, err := batch.Run(bg); err != nil {
			b.Fatal(err)
		}
	}
}

// From a write's request to its change arriving in a live table on
// another connection: the commit, the change capture and the delivery.
func BenchmarkLiveDelivery(b *testing.B) {
	db := benchDB(b, 0)
	f := engine(b)
	viewer := f.connect(b)
	bg := context.Background()
	live, err := azora.Live[benchRow](bg, viewer, azora.TableSpec("t_bench"))
	require.NoError(b, err)
	b.Cleanup(func() { live.Close() })
	require.NoError(b, live.Wait(bg))
	arrived := make(chan struct{}, 1)
	stop := live.OnUpdate(func(u azora.LiveUpdate[benchRow]) {
		if !u.Reset {
			arrived <- struct{}{}
		}
	})
	defer stop()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := db.Exec(bg, "INSERT INTO t_bench(customer_id, status, total) VALUES (1, 'open', 1)"); err != nil {
			b.Fatal(err)
		}
		select {
		case <-arrived:
		case <-time.After(10 * time.Second):
			b.Fatal("the change did not arrive")
		}
	}
}

// Decoding alone, without a server: 1,000 rows into structs.
func BenchmarkCollectStructs(b *testing.B) {
	vals := make([][]any, 1000)
	for i := range vals {
		vals[i] = []any{int64(i), int64(i % 100), "open", float64(i) * 1.5}
	}
	cols := []string{"id", "customer_id", "status", "total"}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out, err := azora.Collect[benchRow](azora.NewRows(cols, vals))
		if err != nil || len(out) != 1000 {
			b.Fatal(err)
		}
	}
}

