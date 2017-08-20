package storage

import (
	"golang.org/x/net/context"

	"github.com/alternative-storage/torus"
	"github.com/dgraph-io/badger"
)

var _ torus.BlockStore = &badgerBlock{}

func init() {
	torus.RegisterBlockStore("badger", openBadgerStorage)
}

func openBadgerStorage(name string, cfg torus.Config, gmd torus.GlobalMetadata) (torus.BlockStore, error) {
	nBlocks := cfg.StorageSize / gmd.BlockSize
	promBlocksAvail.WithLabelValues(name).Set(float64(nBlocks))

	opt := badger.DefaultOptions
	opt.Dir = cfg.DataDir
	opt.ValueDir = cfg.DataDir

	open, err := badger.NewKV(&opt)
	if err != nil {
		return nil, err
	}
	return &badgerBlock{
		nBlocks:   nBlocks,
		name:      name,
		blockSize: gmd.BlockSize,
		kv:        open,
	}, nil
}

type badgerBlock struct {
	closed    bool
	name      string
	blockSize uint64
	nBlocks   uint64
	kv        *badger.KV
}

func (m *badgerBlock) Kind() string { return "badger" }
func (m *badgerBlock) Flush() error { return nil }
func (m *badgerBlock) Close() error {
	if m.kv != nil {
		return m.kv.Close()
	}
	return nil
}

func (m *badgerBlock) HasBlock(_ context.Context, s torus.BlockRef) (bool, error) {
	return m.kv.Exists(s.ToBytes())
}

func (m *badgerBlock) GetBlock(_ context.Context, s torus.BlockRef) ([]byte, error) {
	var item badger.KVItem
	if err := m.kv.Get(s.ToBytes(), &item); err != nil {
		return nil, err
	}
	return item.Value(), nil
}

func (m *badgerBlock) WriteBlock(_ context.Context, s torus.BlockRef, data []byte) error {
	if m.kv == nil {
		promBlockWritesFailed.WithLabelValues(m.name).Inc()
		return torus.ErrClosed
	}
	// TODO: consider CompareAndSet
	if err := m.kv.Set(s.ToBytes(), data, 0x00); err != nil {
		return err
	}

	// promBlocks.WithLabelValues(m.name).Set(float64(len(t.store)))
	promBlocksWritten.WithLabelValues(m.name).Inc()

	return nil
}

// TODO: WriteBuf is necessary for tdp
func (m *badgerBlock) WriteBuf(_ context.Context, s torus.BlockRef) ([]byte, error) { return nil, nil }

func (m *badgerBlock) DeleteBlock(_ context.Context, s torus.BlockRef) error {
	return m.kv.Delete(s.ToBytes())
}

func (m *badgerBlock) BlockIterator() torus.BlockIterator {
	opt := badger.DefaultIteratorOptions
	itr := m.kv.NewIterator(opt)
	return &badgerIterator{
		itr: itr,
	}
}

type badgerIterator struct {
	itr *badger.Iterator
}

func (i *badgerIterator) Err() error { return nil }

func (i *badgerIterator) Next() bool {
	if !i.itr.Valid() {
		return false
	}
	i.itr.Next()
	return true
}
func (i *badgerIterator) BlockRef() torus.BlockRef {
	return torus.BlockRefFromBytes(i.itr.Item().Key())
}

func (i *badgerIterator) Close() error {
	if i.itr != nil {
		i.itr.Close()
	}
	return nil
}

func (m *badgerBlock) NumBlocks() uint64  { return m.nBlocks }
func (m *badgerBlock) BlockSize() uint64  { return m.blockSize }
func (m *badgerBlock) UsedBlocks() uint64 { return 0 }
