package ethdb

import (
	"context"
	"fmt"
	"time"

	"github.com/google/btree"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/metrics"
)

// Implements ethdb.Getter for Tx
type roTxDb struct {
	top bool
	tx  Tx
}

func NewRoTxDb(tx Tx) *roTxDb {
	return &roTxDb{tx: tx, top: true}
}

func (m *roTxDb) Get(bucket string, key []byte) ([]byte, error) {
	c, err := m.tx.Cursor(bucket)
	if err != nil {
		return nil, err
	}
	defer c.Close()
	_, v, err := c.SeekExact(key)
	if err != nil {
		return nil, err
	}
	if v == nil {
		return nil, ErrKeyNotFound
	}
	return v, nil
}

func (m *roTxDb) Has(bucket string, key []byte) (bool, error) {
	c, err := m.tx.Cursor(bucket)
	if err != nil {
		return false, err
	}
	defer c.Close()
	_, v, err := c.SeekExact(key)

	return v != nil, err
}

func (m *roTxDb) Walk(bucket string, startkey []byte, fixedbits int, walker func([]byte, []byte) (bool, error)) error {
	c, err := m.tx.Cursor(bucket)
	if err != nil {
		return err
	}
	defer c.Close()
	return Walk(c, startkey, fixedbits, walker)
}

func (m *roTxDb) BeginGetter(ctx context.Context) (GetterTx, error) {
	return &roTxDb{tx: m.tx, top: false}, nil
}

func (m *roTxDb) Rollback() {
	if m.top {
		m.tx.Rollback()
	}
}

func (m *roTxDb) Tx() Tx {
	return m.tx
}

// TxDb - provides Database interface around ethdb.Tx
// It's not thread-safe!
// TxDb not usable after .Commit()/.Rollback() call, but usable after .CommitAndBegin() call
// you can put unlimited amount of data into this class
// Walk and MultiWalk methods - work outside of Tx object yet, will implement it later
type TxDb struct {
	db      Database
	tx      Tx
	txFlags TxFlags
	cursors map[string]Cursor
	len     uint64
}

func (m *TxDb) Close() {
	panic("don't call me")
}

// NewTxDbWithoutTransaction creates TxDb object without opening transaction,
// such TxDb not usable before .Begin() call on it
// It allows inject TxDb object into class hierarchy, but open write transaction later
func NewTxDbWithoutTransaction(db Database, flags TxFlags) DbWithPendingMutations {
	return &TxDb{db: db, txFlags: flags}
}

func (m *TxDb) Begin(ctx context.Context, flags TxFlags) (DbWithPendingMutations, error) {
	batch := m
	if m.tx != nil {
		panic("nested transactions not supported")
	}

	if err := batch.begin(ctx, flags); err != nil {
		return nil, err
	}
	return batch, nil
}

func (m *TxDb) BeginGetter(ctx context.Context) (GetterTx, error) {
	batch := m
	if m.tx != nil {
		panic("nested transactions not supported")
	}

	if err := batch.begin(ctx, RO); err != nil {
		return nil, err
	}
	return batch, nil
}

func (m *TxDb) cursor(bucket string) (Cursor, error) {
	c, ok := m.cursors[bucket]
	if !ok {
		var err error
		c, err = m.tx.Cursor(bucket)
		if err != nil {
			return nil, err
		}
		m.cursors[bucket] = c
	}
	return c, nil
}

func (m *TxDb) IncrementSequence(bucket string, amount uint64) (res uint64, err error) {
	return m.tx.(RwTx).IncrementSequence(bucket, amount)
}

func (m *TxDb) ReadSequence(bucket string) (res uint64, err error) {
	return m.tx.ReadSequence(bucket)
}

func (m *TxDb) Put(bucket string, key []byte, value []byte) error {
	m.len += uint64(len(key) + len(value))
	c, err := m.cursor(bucket)
	if err != nil {
		return err
	}
	return c.(RwCursor).Put(key, value)
}

func (m *TxDb) Append(bucket string, key []byte, value []byte) error {
	m.len += uint64(len(key) + len(value))
	c, err := m.cursor(bucket)
	if err != nil {
		return err
	}
	return c.(RwCursor).Append(key, value)
}

func (m *TxDb) AppendDup(bucket string, key []byte, value []byte) error {
	m.len += uint64(len(key) + len(value))
	c, err := m.cursor(bucket)
	if err != nil {
		return err
	}
	return c.(RwCursorDupSort).AppendDup(key, value)
}

func (m *TxDb) Delete(bucket string, k, v []byte) error {
	m.len += uint64(len(k))
	c, err := m.cursor(bucket)
	if err != nil {
		return err
	}
	return c.(RwCursor).Delete(k, v)
}

func (m *TxDb) NewBatch() DbWithPendingMutations {
	return &mutation{
		db:   m,
		puts: btree.New(32),
	}
}

func (m *TxDb) begin(ctx context.Context, flags TxFlags) error {
	kv := m.db.(HasRwKV).RwKV()

	var tx Tx
	var err error
	if flags&RO != 0 {
		tx, err = kv.BeginRo(ctx)
	} else {
		tx, err = kv.BeginRw(ctx)
	}
	if err != nil {
		return err
	}
	m.tx = tx
	m.cursors = make(map[string]Cursor, 16)
	return nil
}

func (m *TxDb) RwKV() RwKV {
	panic("not allowed to get KV interface because you will loose transaction, please use .Tx() method")
}

// Last can only be called from the transaction thread
func (m *TxDb) Last(bucket string) ([]byte, []byte, error) {
	c, err := m.cursor(bucket)
	if err != nil {
		return []byte{}, nil, err
	}
	return c.Last()
}

func (m *TxDb) Get(bucket string, key []byte) ([]byte, error) {
	c, err := m.cursor(bucket)
	if err != nil {
		return nil, err
	}
	_, v, err := c.SeekExact(key)
	if err != nil {
		return nil, err
	}
	if v == nil {
		return nil, ErrKeyNotFound
	}
	return v, nil
}

func (m *TxDb) Has(bucket string, key []byte) (bool, error) {
	v, err := m.Get(bucket, key)
	if err != nil {
		return false, err
	}
	return v != nil, nil
}

func (m *TxDb) DiskSize(ctx context.Context) (common.StorageSize, error) {
	if m.db == nil {
		return 0, nil
	}
	sz, err := m.db.(HasStats).DiskSize(ctx)
	if err != nil {
		return 0, err
	}
	return common.StorageSize(sz), nil
}

func (m *TxDb) MultiPut(tuples ...[]byte) (uint64, error) {
	return 0, MultiPut(m.tx.(RwTx), tuples...)
}

func (m *TxDb) BatchSize() int {
	return int(m.len)
}

func (m *TxDb) Walk(bucket string, startkey []byte, fixedbits int, walker func([]byte, []byte) (bool, error)) error {
	m.panicOnEmptyDB()
	// get cursor out of pool, then calls txDb.Put/Get/Delete on same bucket inside Walk callback - will not affect state of Walk
	c, ok := m.cursors[bucket]
	if ok {
		delete(m.cursors, bucket)
	} else {
		var err error
		c, err = m.tx.Cursor(bucket)
		if err != nil {
			return err
		}
	}
	defer func() { // put cursor back to pool if can
		if _, ok = m.cursors[bucket]; ok {
			c.Close()
		} else {
			m.cursors[bucket] = c
		}
	}()
	return Walk(c, startkey, fixedbits, walker)
}

func (m *TxDb) CommitAndBegin(ctx context.Context) error {
	err := m.Commit()
	if err != nil {
		return err
	}

	return m.begin(ctx, m.txFlags)
}

func (m *TxDb) RollbackAndBegin(ctx context.Context) error {
	m.Rollback()
	return m.begin(ctx, m.txFlags)
}

func (m *TxDb) Commit() error {
	if metrics.Enabled {
		defer dbCommitBigBatchTimer.UpdateSince(time.Now())
	}

	if m.tx == nil {
		return fmt.Errorf("second call .Commit() on same transaction")
	}
	if err := m.tx.Commit(); err != nil {
		return err
	}
	m.tx = nil
	m.cursors = nil
	m.len = 0
	return nil
}

func (m *TxDb) Rollback() {
	if m.tx == nil {
		return
	}
	m.tx.Rollback()
	m.cursors = nil
	m.tx = nil
	m.len = 0
}

func (m *TxDb) Tx() Tx {
	return m.tx
}

func (m *TxDb) Keys() ([][]byte, error) {
	panic("don't use me")
}

func (m *TxDb) panicOnEmptyDB() {
	if m.db == nil {
		panic("Not implemented")
	}
}

func (m *TxDb) BucketExists(name string) (bool, error) {
	exists := false
	migrator, ok := m.tx.(BucketMigrator)
	if !ok {
		return false, fmt.Errorf("%T doesn't implement ethdb.TxMigrator interface", m.tx)
	}
	exists = migrator.ExistsBucket(name)
	return exists, nil
}

func (m *TxDb) ClearBuckets(buckets ...string) error {
	for i := range buckets {
		name := buckets[i]

		migrator, ok := m.tx.(BucketMigrator)
		if !ok {
			return fmt.Errorf("%T doesn't implement ethdb.TxMigrator interface", m.tx)
		}
		if err := migrator.ClearBucket(name); err != nil {
			return err
		}
	}

	return nil
}

func (m *TxDb) DropBuckets(buckets ...string) error {
	for i := range buckets {
		name := buckets[i]
		log.Info("Dropping bucket", "name", name)
		migrator, ok := m.tx.(BucketMigrator)
		if !ok {
			return fmt.Errorf("%T doesn't implement ethdb.TxMigrator interface", m.tx)
		}
		if err := migrator.DropBucket(name); err != nil {
			return err
		}
	}
	return nil
}
