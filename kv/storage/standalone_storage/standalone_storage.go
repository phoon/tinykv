package standalone_storage

import (
	"errors"

	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).

	store *badger.DB
	conf  *config.Config
}

var _ storage.Storage = (*StandAloneStorage)(nil)

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).

	return &StandAloneStorage{
		conf: conf,
	}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).

	if s.store = engine_util.CreateDB(s.conf.DBPath, s.conf.Raft); s.store == nil {
		return errors.New("failed to create db")
	}
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).

	return s.store.Close()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).

	return &StandAloneReader{
		txn: s.store.NewTransaction(false), // readonly
	}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).

	for _, m := range batch {
		switch m.Data.(type) {
		case storage.Put:
			engine_util.PutCF(s.store, m.Cf(), m.Key(), m.Value())
		case storage.Delete:
			engine_util.DeleteCF(s.store, m.Cf(), m.Key())
		}
	}
	return nil
}

// StandAloneReader implements the StorageReader interface
type StandAloneReader struct {
	txn *badger.Txn
}

var _ storage.StorageReader = (*StandAloneReader)(nil)

func (sr *StandAloneReader) GetCF(cf string, key []byte) ([]byte, error) {
	// follows the implementation from `mem_storage.go`
	// that satisfy the RawGet test
	v, _ := engine_util.GetCFFromTxn(sr.txn, cf, key)

	if v == nil {
		return nil, nil
	}

	return v, nil
}

func (sr *StandAloneReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, sr.txn)
}

func (sr *StandAloneReader) Close() {
	sr.txn.Discard()
}
