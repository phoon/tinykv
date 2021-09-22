package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).

	r, err := server.storage.Reader(req.GetContext())
	if err != nil {
		return nil, err
	}
	defer r.Close()

	v, err := r.GetCF(req.Cf, req.Key)
	if err != nil {
		return nil, err
	}

	return &kvrpcpb.RawGetResponse{
		Value:    v,
		NotFound: v == nil,
	}, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified

	m := storage.Modify{
		Data: storage.Put{
			Cf:    req.Cf,
			Key:   req.Key,
			Value: req.Value,
		},
	}
	err := server.storage.Write(req.GetContext(), []storage.Modify{m})
	if err != nil {
		return nil, err
	}
	return &kvrpcpb.RawPutResponse{}, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted

	m := storage.Modify{
		Data: storage.Delete{
			Cf:  req.Cf,
			Key: req.Key,
		},
	}
	err := server.storage.Write(req.GetContext(), []storage.Modify{m})
	if err != nil {
		return nil, err
	}

	return &kvrpcpb.RawDeleteResponse{}, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF

	r, err := server.storage.Reader(req.GetContext())
	defer r.Close()
	if err != nil {
		return nil, err
	}

	itr := r.IterCF(req.Cf)
	defer itr.Close()

	var kvPs []*kvrpcpb.KvPair
	itr.Seek(req.StartKey)
	for i := 0; i < int(req.GetLimit()); i++ {
		if itr.Valid() {
			k := itr.Item().Key()
			v, err := itr.Item().Value()
			if err != nil {
				return nil, err
			}
			kvPs = append(kvPs, &kvrpcpb.KvPair{
				Key:   k,
				Value: v,
			})
			itr.Next()
		}
	}

	return &kvrpcpb.RawScanResponse{Kvs: kvPs}, nil
}
