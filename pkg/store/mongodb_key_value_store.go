package store

import (
	"context"
	"fmt"
	"os"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/debug"
	"sharedlog-stream/pkg/exactly_once_intr"
	"sharedlog-stream/pkg/utils"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readconcern"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"
)

const (
	KEY_NAME   = "k"
	VALUE_NAME = "v"
)

type MongoDBKeyValueStore struct {
	session       mongo.Session
	sessCtx       mongo.SessionContext
	config        *MongoDBConfig
	indexCreation map[string]struct{}
	op_log        []kv_op
	inTransaction bool
}

type MongoDBConfig struct {
	ValueSerde     commtypes.Serde
	KeySerde       commtypes.Serde
	Client         *mongo.Client
	CollectionName string

	DBName string
}

type KV_OP uint8

const (
	PUT    KV_OP = 0
	DELETE KV_OP = 1
)

type kv_op struct {
	op         KV_OP
	key        []byte
	val        []byte
	collection string
}

var _ = KeyValueStore(&MongoDBKeyValueStore{})

func NewMongoDBKeyValueStore(ctx context.Context, config *MongoDBConfig) (*MongoDBKeyValueStore, error) {
	debug.Assert(config.Client != nil, "mongo db connection should be created first")
	debug.Assert(config.CollectionName != "", "collection name should not be empty")
	debug.Assert(config.DBName != "", "db name should not be empty")
	/*
		col := client.Database(config.DBName).Collection(config.CollectionName)

			idxView := col.Indexes()
			_, err = idxView.CreateOne(ctx, mongo.IndexModel{
				Keys:    bson.M{KEY_NAME: 1},
				Options: options.Index().SetName("kv"),
			})
			if err != nil {
				return nil, err
			}
	*/
	return &MongoDBKeyValueStore{
		config:        config,
		indexCreation: make(map[string]struct{}),
		inTransaction: false,
	}, nil
}

func (s *MongoDBKeyValueStore) DropDatabase(ctx context.Context) error {
	return s.config.Client.Database(s.config.DBName).Drop(ctx)
}

func (s *MongoDBKeyValueStore) StartTransaction(ctx context.Context) error {
	session, err := s.config.Client.StartSession()
	if err != nil {
		debug.Fprintf(os.Stderr, "fail to start session\n")
		return err
	}
	s.session = session
	sessCtx := mongo.NewSessionContext(ctx, session)
	s.sessCtx = sessCtx
	tranOpts := options.Transaction().
		SetReadPreference(readpref.Primary()).
		SetReadConcern(readconcern.Snapshot()).
		SetWriteConcern(writeconcern.New(writeconcern.WMajority()))
	if err = session.StartTransaction(tranOpts); err != nil {
		debug.Fprintf(os.Stderr, "fail to start transaction\n")
		return err
	}
	s.inTransaction = true
	s.op_log = make([]kv_op, 0, 100)
	return nil
}

func (s *MongoDBKeyValueStore) CommitTransaction(ctx context.Context,
	taskRepr string, transactionID uint64) error {
	err := RunTranFuncWithRetry(s.sessCtx, func(sc mongo.SessionContext) error {
		col := s.config.Client.Database(taskRepr).Collection(taskRepr)
		debug.Fprintf(os.Stderr, "update tranID to %d for db %s task %s\n", transactionID, taskRepr, taskRepr)
		opts := options.Update().SetUpsert(true)
		_, err := col.UpdateOne(sc, bson.M{KEY_NAME: "tranID"},
			bson.M{"$set": bson.M{VALUE_NAME: transactionID}}, opts)
		if err != nil {
			if cmdErr, ok := err.(mongo.CommandError); ok && !cmdErr.HasErrorLabel("TransientTransactionError") {
				debug.Fprintf(os.Stderr, "abort transaction due to err of UpdateOne: %v\n", err)
				_ = sc.AbortTransaction(context.Background())
			}
		}
		return err
	})
	if err != nil {
		return err
	}

	if err := CommitWithRetry(s.sessCtx); err != nil {
		if cmdErr, ok := err.(mongo.CommandError); ok && cmdErr.HasErrorLabel("TransientTransactionError") {
			return s.redoTransaction(ctx)
		}
		return err
	}
	s.session.EndSession(s.sessCtx)
	s.inTransaction = false
	s.sessCtx = nil
	debug.Fprintf(os.Stderr, "%s transaction committed\n", s.Name())
	return nil
}

func (s *MongoDBKeyValueStore) redoTransaction(ctx context.Context) error {
	for {
		err := s.StartTransaction(ctx)
		if err != nil {
			return err
		}
		err = s.replayOpLog(ctx)
		if err != nil {
			return err
		}
		err = CommitWithRetry(s.sessCtx)
		if err == nil {
			return nil
		}
		if cmdErr, ok := err.(mongo.CommandError); ok && cmdErr.HasErrorLabel("TransientTransactionError") {
			continue
		}
		if err != nil {
			return err
		}
	}
}

func (s *MongoDBKeyValueStore) AbortTransaction(ctx context.Context) error {
	if err := s.session.AbortTransaction(ctx); err != nil {
		return err
	}
	s.session.EndSession(ctx)
	s.inTransaction = false
	s.sessCtx = nil
	debug.Fprintf(os.Stderr, "%s transaction aborted", s.Name())
	return nil
}

func (s *MongoDBKeyValueStore) IsOpen() bool {
	return true
}

func (s *MongoDBKeyValueStore) Init(ctx StoreContext) {
}

func (s *MongoDBKeyValueStore) Name() string {
	return s.config.DBName
}

func (s *MongoDBKeyValueStore) Get(ctx context.Context, key commtypes.KeyT) (commtypes.ValueT, bool, error) {
	var err error
	kBytes, ok := key.([]byte)
	if !ok {
		kBytes, err = s.config.KeySerde.Encode(key)
		if err != nil {
			return nil, false, fmt.Errorf("Get: key encode err %v", err)
		}
	}
	valBytes, ok, err := s.GetWithCollection(ctx, kBytes, s.config.CollectionName)
	if err != nil {
		return nil, false, fmt.Errorf("Get: get with collection err %v", err)
	}
	if ok {
		val, err := s.config.ValueSerde.Decode(valBytes)
		if err != nil {
			debug.Fprintf(os.Stderr, "Get val bytes: %v", valBytes)
			return nil, false, fmt.Errorf("Get: decode err %v", err)
		}
		return val, ok, nil
	} else {
		return nil, ok, nil
	}
}

func (s *MongoDBKeyValueStore) GetWithCollection(ctx context.Context, kBytes []byte, collection string) ([]byte, bool, error) {
	var err error
	var result bson.M

	if !s.inTransaction {
		col := s.config.Client.Database(s.config.DBName).Collection(collection)
		err = col.FindOne(ctx, bson.M{KEY_NAME: kBytes}).Decode(&result)
	} else {
		debug.Assert(s.sessCtx != nil, "session ctx should not be nil")
		debug.Assert(s.session != nil, "session should not be nil")
		err = RunTranFuncWithRetry(s.sessCtx, func(sc mongo.SessionContext) error {
			col := s.config.Client.Database(s.config.DBName).Collection(collection)
			err = col.FindOne(sc, bson.M{KEY_NAME: kBytes}).Decode(&result)
			if err != nil {
				if err != mongo.ErrNoDocuments {
					if cmdErr, ok := err.(mongo.CommandError); ok && !cmdErr.HasErrorLabel("TransientTransactionError") {
						debug.Fprintf(os.Stderr, "abort transaction due to err of FindOne: %v\n", err)
						_ = s.sessCtx.AbortTransaction(context.Background())
					}
				}
			}
			return err
		})
	}
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, false, nil
		}
		return nil, false, err
	}
	valBytes := result[VALUE_NAME]
	if valBytes == nil {
		return nil, true, nil
	}
	return valBytes.(primitive.Binary).Data, true, nil
}

func (s *MongoDBKeyValueStore) RangeWithCollection(ctx context.Context,
	fromBytes []byte, toBytes []byte, collection string,
	iterFunc func([]byte, []byte) error,
) error {
	var err error
	var cur *mongo.Cursor

	var condition bson.M
	if fromBytes == nil && toBytes == nil {
		condition = bson.M{}
	} else if fromBytes == nil && toBytes != nil {
		condition = bson.M{KEY_NAME: bson.M{"$lte": toBytes}}
	} else if fromBytes != nil && toBytes == nil {
		condition = bson.M{
			KEY_NAME: bson.M{"$gte": fromBytes}}
	} else {
		condition = bson.M{KEY_NAME: bson.M{"$gte": fromBytes, "$lte": toBytes}}
	}
	if !s.inTransaction {
		opts := options.Find().SetSort(bson.M{KEY_NAME: 1})
		col := s.config.Client.Database(s.config.DBName).Collection(collection)
		cur, err = col.Find(ctx, condition, opts)
	} else {
		debug.Assert(s.sessCtx != nil, "session ctx should not be nil")
		debug.Assert(s.session != nil, "session should not be nil")
		err = RunTranFuncWithRetry(s.sessCtx, func(sc mongo.SessionContext) error {
			opts := options.Find().SetSort(bson.M{KEY_NAME: 1})
			col := s.config.Client.Database(s.config.DBName).Collection(collection)
			cur, err = col.Find(sc, condition, opts)
			if err != nil {
				if err != mongo.ErrNoDocuments {
					if cmdErr, ok := err.(mongo.CommandError); ok && !cmdErr.HasErrorLabel("TransientTransactionError") {
						debug.Fprintf(os.Stderr, "abort transaction due to err of Find: %v\n", err)
						_ = sc.AbortTransaction(context.Background())
					}
				}
			}
			return err
		})
	}
	if err != nil {
		if err == mongo.ErrNoDocuments {
			fmt.Fprint(os.Stderr, "can't find the document\n")
			return nil
		}
		return err
	}

	var res []bson.M
	err = cur.All(ctx, &res)
	if err != nil {
		return err
	}
	for _, r := range res {
		kBytes := r[KEY_NAME].(primitive.Binary).Data
		vBytes := r[VALUE_NAME].(primitive.Binary).Data
		err = iterFunc(kBytes, vBytes)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *MongoDBKeyValueStore) Range(ctx context.Context, from commtypes.KeyT, to commtypes.KeyT,
	iterFunc func(commtypes.KeyT, commtypes.ValueT) error,
) error {
	fromBytes, err := utils.ConvertToBytes(from, s.config.KeySerde)
	if err != nil {
		return err
	}
	toBytes, err := utils.ConvertToBytes(to, s.config.KeySerde)
	if err != nil {
		return err
	}
	return s.RangeWithCollection(ctx, fromBytes, toBytes, s.config.CollectionName, func(kBytes, vBytes []byte) error {
		key, err := s.config.KeySerde.Decode(kBytes)
		if err != nil {
			return err
		}
		val, err := s.config.ValueSerde.Decode(vBytes)
		if err != nil {
			return err
		}
		return iterFunc(key, val)
	})
}

func (s *MongoDBKeyValueStore) ReverseRange(from commtypes.KeyT, to commtypes.KeyT, iterFunc func(commtypes.KeyT, commtypes.ValueT) error) error {
	panic("not implemented")
}

func (s *MongoDBKeyValueStore) ApproximateNumEntriesWithCollection(
	ctx context.Context, collection_name string,
) (uint64, error) {
	panic("not implemented")
}

func (s *MongoDBKeyValueStore) ApproximateNumEntries(ctx context.Context) (uint64, error) {
	panic("not implemented")
}

func (s *MongoDBKeyValueStore) Put(ctx context.Context, key commtypes.KeyT, value commtypes.ValueT) error {
	kBytes, err := utils.ConvertToBytes(key, s.config.KeySerde)
	if err != nil {
		return err
	}
	vBytes, err := utils.ConvertToBytes(value, s.config.ValueSerde)
	if err != nil {
		return err
	}
	return s.PutWithCollection(ctx, kBytes, vBytes, s.config.CollectionName)
}

func (s *MongoDBKeyValueStore) PutWithoutPushToChangelog(ctx context.Context, key commtypes.KeyT, value commtypes.ValueT) error {
	return s.Put(ctx, key, value)
}

func (s *MongoDBKeyValueStore) PutWithCollection(ctx context.Context, kBytes []byte,
	vBytes []byte, collection string,
) error {
	if vBytes == nil {
		return s.DeleteWithCollection(ctx, kBytes, collection)
	} else {
		var err error
		/*
			_, ok := s.indexCreation[collection]
			if !ok {
				idxView := col.Indexes()
				_, err := idxView.CreateOne(ctx, mongo.IndexModel{
					Keys:    bson.M{KEY_NAME: 1},
					Options: options.Index().SetName("kv"),
				})
				if err != nil {
					return err
				}
				s.indexCreation[collection] = struct{}{}
			}
		*/
		// fmt.Fprintf(os.Stderr, "put k: %v, val: %v\n", kBytes, vBytes)
		opts := options.Update().SetUpsert(true)
		if !s.inTransaction {
			col := s.config.Client.Database(s.config.DBName).Collection(collection)
			_, err = col.UpdateOne(ctx, bson.M{KEY_NAME: kBytes},
				bson.M{"$set": bson.M{VALUE_NAME: vBytes}}, opts)
		} else {
			err = RunTranFuncWithRetry(s.sessCtx, func(sc mongo.SessionContext) error {
				col := s.config.Client.Database(s.config.DBName).Collection(collection)
				_, err = col.UpdateOne(sc, bson.M{KEY_NAME: kBytes},
					bson.M{"$set": bson.M{VALUE_NAME: vBytes}}, opts)
				if err != nil {
					if cmdErr, ok := err.(mongo.CommandError); ok && !cmdErr.HasErrorLabel("TransientTransactionError") {
						debug.Fprintf(os.Stderr, "abort transaction due to err of UpdateOne in Put: %v\n", err)
						_ = sc.AbortTransaction(context.Background())
					}
				}
				return err
			})
			s.op_log = append(s.op_log, kv_op{op: PUT, key: kBytes, val: vBytes, collection: collection})
		}
		return err
	}
}

func (s *MongoDBKeyValueStore) PutIfAbsent(ctx context.Context, key commtypes.KeyT, value commtypes.ValueT) (commtypes.ValueT, error) {
	// assume key and value to be bytes
	panic("not implemented")
}

func (s *MongoDBKeyValueStore) PutAll(ctx context.Context, entries []*commtypes.Message) error {
	panic("not implemented")
}

func (s *MongoDBKeyValueStore) Delete(ctx context.Context, key commtypes.KeyT) error {
	kBytes, err := s.config.KeySerde.Encode(key)
	if err != nil {
		return err
	}
	return s.DeleteWithCollection(ctx, kBytes, s.config.CollectionName)
}

func (s *MongoDBKeyValueStore) DeleteWithCollection(ctx context.Context,
	kBytes []byte, collection string,
) error {
	col := s.config.Client.Database(s.config.DBName).Collection(collection)
	ctx_tmp := ctx
	if s.inTransaction {
		debug.Assert(s.sessCtx != nil, "session ctx should not be nil")
		debug.Assert(s.session != nil, "session should not be nil")
		ctx_tmp = s.sessCtx
		s.op_log = append(s.op_log, kv_op{op: DELETE, key: kBytes, val: nil, collection: collection})
	}
	debug.Fprintf(os.Stderr, "delete %v in col %s", kBytes, collection)
	_, err := col.DeleteOne(ctx_tmp, bson.M{KEY_NAME: kBytes})
	if err != nil {
		if s.inTransaction {
			debug.Fprintf(os.Stderr, "abort transaction due to err of DeleteOne: %v\n", err)
			_ = s.sessCtx.AbortTransaction(context.Background())
		}
		return err
	}
	return nil
}

func (s *MongoDBKeyValueStore) replayOpLog(ctx context.Context) error {
	for _, log_entry := range s.op_log {
		if log_entry.op == PUT {
			err := s.PutWithCollection(ctx, log_entry.key, log_entry.val, log_entry.collection)
			if err != nil {
				return err
			}
		} else if log_entry.op == DELETE {
			err := s.DeleteWithCollection(ctx, log_entry.key, log_entry.collection)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *MongoDBKeyValueStore) PrefixScan(prefix interface{}, prefixKeyEncoder commtypes.Encoder,
	iterFunc func(commtypes.KeyT, commtypes.ValueT) error,
) error {
	panic("not implemented")
}

func (s *MongoDBKeyValueStore) TableType() TABLE_TYPE {
	return MONGODB
}

func (s *MongoDBKeyValueStore) GetTransactionID(ctx context.Context, taskRepr string) (uint64, bool, error) {
	col := s.config.Client.Database(taskRepr).Collection(taskRepr)
	var result bson.M
	err := col.FindOne(ctx, bson.M{KEY_NAME: "tranID"}).Decode(&result)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return 0, false, nil
		}
		return 0, false, err
	}
	val := result[VALUE_NAME]
	return uint64(val.(int64)), true, nil
}

func (s *MongoDBKeyValueStore) SetTrackParFunc(exactly_once_intr.TrackProdSubStreamFunc) {}
