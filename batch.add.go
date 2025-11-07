package mychgorm

import (
	"context"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2"
	"strings"
	"sync"
	"time"
)

// ==================== BatchWriter ====================
type BatchWriter struct {
	conn   clickhouse.Conn
	tables map[string]*tableBuffer
	cfg    BatchWriterConfig
	mu     sync.Mutex
	wg     sync.WaitGroup
}

func NewBatchWriter(chConn clickhouse.Conn, cfg BatchWriterConfig) *BatchWriter {
	return &BatchWriter{
		conn:   chConn,
		tables: make(map[string]*tableBuffer),
		cfg:    cfg,
	}
}

func (bw *BatchWriter) Write(record InsertSQL) {
	table := record.TableName()

	bw.mu.Lock()
	tb, exists := bw.tables[table]
	if !exists {
		tb = newTableBuffer(bw.conn, table, record.InsertSQLFields(), bw.cfg, &bw.wg)
		bw.tables[table] = tb
	}
	bw.mu.Unlock()

	tb.add(record)
}

func (bw *BatchWriter) Close() {
	bw.mu.Lock()
	for _, tb := range bw.tables {
		tb.stop()
	}
	bw.mu.Unlock()

	bw.wg.Wait()
	fmt.Println("[BatchWriter] all tables flushed and stopped")
}

// ==================== BatchSyncWriter ====================
type BatchSyncWriter struct {
	conn   clickhouse.Conn
	cfg    BatchWriterConfig
	ch     chan InsertSQL
	tables map[string]*tableBuffer
	mu     sync.Mutex
	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc
}

func NewBatchSyncWriter(conn clickhouse.Conn, cfg BatchWriterConfig) *BatchSyncWriter {
	ctx, cancel := context.WithCancel(context.Background())
	bs := &BatchSyncWriter{
		conn:   conn,
		cfg:    cfg,
		ch:     make(chan InsertSQL, cfg.MaxInsertCacheNum+100_000),
		tables: make(map[string]*tableBuffer),
		ctx:    ctx,
		cancel: cancel,
	}
	bs.wg.Add(1)
	go bs.run()
	return bs
}

// Write 写入统一 channel
func (bs *BatchSyncWriter) Write(record InsertSQL) {
	select {
	case bs.ch <- record:
	default:
		fmt.Printf("[WARN] BatchSyncWriter channel full, dropping record for table %s", record.TableName())
	}
}

// run 后台拉取 channel 数据，分发到对应 tableBuffer
func (bs *BatchSyncWriter) run() {
	defer bs.wg.Done()
	for {
		select {
		case <-bs.ctx.Done():
			// flush channel中的剩余数据
			for {
				select {
				case rec := <-bs.ch:
					bs.getTableBuffer(rec).add(rec)
				default:
					bs.flushAll()
					return
				}
			}
		case rec := <-bs.ch:
			tb := bs.getTableBuffer(rec)
			tb.add(rec)
		}
	}
}

func (bs *BatchSyncWriter) getTableBuffer(record InsertSQL) *tableBuffer {
	table := record.TableName()
	bs.mu.Lock()
	defer bs.mu.Unlock()
	tb, exists := bs.tables[table]
	if !exists {
		tb = newTableBuffer(bs.conn, table, record.InsertSQLFields(), bs.cfg, &bs.wg)
		bs.tables[table] = tb
	}
	return tb
}

func (bs *BatchSyncWriter) flushAll() {
	bs.mu.Lock()
	defer bs.mu.Unlock()
	for _, tb := range bs.tables {
		tb.flush()
	}
}

// Close 停止后台 goroutine 并 flush 所有数据
func (bs *BatchSyncWriter) Close() {
	bs.cancel()
	bs.wg.Wait()
	fmt.Println("[BatchSyncWriter] all tables flushed and stopped")
}

// =====TableBuffer:每张表独立缓存 ====================//
type tableBuffer struct {
	table  string
	fields []string
	cache  []InsertSQL
	mu     sync.Mutex
	conn   clickhouse.Conn
	timer  *time.Ticker
	ctx    context.Context
	cancel context.CancelFunc
	wg     *sync.WaitGroup
	cfg    BatchWriterConfig
}

func newTableBuffer(conn clickhouse.Conn, table string, fields []string, cfg BatchWriterConfig, wg *sync.WaitGroup) *tableBuffer {
	ctx, cancel := context.WithCancel(context.Background())
	tb := &tableBuffer{
		table:  table,
		fields: fields,
		cache:  make([]InsertSQL, 0, cfg.MaxInsertCacheNum),
		conn:   conn,
		timer:  time.NewTicker(cfg.MaxInsertDuration),
		ctx:    ctx,
		cancel: cancel,
		wg:     wg,
		cfg:    cfg,
	}
	wg.Add(1)
	go tb.run()
	return tb
}

func (tb *tableBuffer) run() {
	defer tb.wg.Done()
	for {
		select {
		case <-tb.ctx.Done():
			tb.flush()
			return
		case <-tb.timer.C:
			tb.flush()
		}
	}
}

func (tb *tableBuffer) add(record InsertSQL) {
	tb.mu.Lock()
	tb.cache = append(tb.cache, record)
	shouldFlush := len(tb.cache) >= tb.cfg.MaxInsertCacheNum
	tb.mu.Unlock()

	if shouldFlush {
		tb.flush()
	}
}

func (tb *tableBuffer) flush() {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	if len(tb.cache) == 0 {
		return
	}

	ctx := context.Background()
	batch, err := tb.conn.PrepareBatch(ctx,
		fmt.Sprintf("INSERT INTO %s (%s)", tb.table, strings.Join(tb.fields, ",")))
	if err != nil {
		fmt.Printf("[ERROR] prepare batch for table %s: %v\n\n", tb.table, err)
		return
	}

	for _, rec := range tb.cache {
		if err := batch.Append(rec.InsertSQLValues()...); err != nil {
			fmt.Printf("[ERROR] append failed for table %s: %v\n\n", tb.table, err)
			return
		}
	}

	if err := batch.Send(); err != nil {
		fmt.Printf("[ERROR] send failed for table %s: %v\n\n", tb.table, err)
	} else {
		fmt.Printf("[OK] inserted %d rows into %s\n\n", len(tb.cache), tb.table)
	}

	tb.cache = tb.cache[:0]
}

func (tb *tableBuffer) stop() {
	tb.cancel()
	tb.timer.Stop()
}
