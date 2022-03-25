package store

import (
	"context"
	"fmt"
	"os"
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"sharedlog-stream/pkg/utils"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readconcern"
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
	client        *mongo.Client
	indexCreation map[string]struct{}
	inTransaction bool
}

type MongoDBConfig struct {
	ValueSerde     commtypes.Serde
	KeySerde       commtypes.Serde
	Addr           string
	CollectionName string

	DBName string
}

var _ = KeyValueStore(&MongoDBKeyValueStore{})

func NewMongoDBKeyValueStore(ctx context.Context, config *MongoDBConfig) (*MongoDBKeyValueStore, error) {
	clientOpts := options.Client().ApplyURI(config.Addr)
	client, err := mongo.Connect(ctx, clientOpts)
	if err != nil {
		return nil, err
	}
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
		client:        client,
		indexCreation: make(map[string]struct{}),
		inTransaction: false,
	}, nil
}

func (s *MongoDBKeyValueStore) DropDatabase(ctx context.Context) error {
	return s.client.Database(s.config.DBName).Drop(ctx)
}

func (s *MongoDBKeyValueStore) StartTransaction(ctx context.Context) error {
	sessionOpts := options.Session().SetDefaultWriteConcern(writeconcern.New(writeconcern.WMajority())).
		SetDefaultReadConcern(readconcern.Linearizable())
	session, err := s.client.StartSession(sessionOpts)
	if err != nil {
		return err
	}
	s.session = session
	sessCtx := mongo.NewSessionContext(ctx, session)
	s.sessCtx = sessCtx
	if err = session.StartTransaction(); err != nil {
		return err
	}
	s.inTransaction = true
	return nil
}

func (s *MongoDBKeyValueStore) CommitTransaction(ctx context.Context, taskRepr string, transactionID uint64) error {
	col := s.client.Database(s.config.DBName).Collection(taskRepr)
	opts := options.Update().SetUpsert(true)
	_, err := col.UpdateOne(s.sessCtx, bson.M{KEY_NAME: "tranID"},
		bson.M{"$set": bson.M{VALUE_NAME: transactionID}}, opts)
	if err != nil {
		err2 := s.sessCtx.AbortTransaction(context.Background())
		if err2 != nil {
			return fmt.Errorf("try to abort transaction while handling err (%v) and abort failed: %v",
				err, err2)
		}
	}
	if err := s.session.CommitTransaction(ctx); err != nil {
		return err
	}
	s.session.EndSession(ctx)
	s.inTransaction = false
	s.sessCtx = nil
	return nil
}

func (s *MongoDBKeyValueStore) AbortTransaction(ctx context.Context) error {
	if err := s.session.AbortTransaction(ctx); err != nil {
		return err
	}
	s.session.EndSession(ctx)
	s.inTransaction = false
	s.sessCtx = nil
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
			return nil, false, err
		}
	}
	valBytes, ok, err := s.GetWithCollection(ctx, kBytes, s.config.CollectionName)
	if err != nil {
		return nil, false, err
	}
	val, err := s.config.ValueSerde.Decode(valBytes)
	if err != nil {
		return nil, false, err
	}
	return val, ok, nil
}

func (s *MongoDBKeyValueStore) GetWithCollection(ctx context.Context, kBytes []byte, collection string) ([]byte, bool, error) {
	var err error
	col := s.client.Database(s.config.DBName).Collection(collection)
	var result bson.M
	ctx_tmp := ctx
	if s.inTransaction {
		ctx_tmp = s.sessCtx
	}
	err = col.FindOne(ctx_tmp, bson.M{KEY_NAME: kBytes}).Decode(&result)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, false, nil
		}
		if s.inTransaction {
			_ = s.sessCtx.AbortTransaction(context.Background())
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
	col := s.client.Database(s.config.DBName).Collection(collection)
	opts := options.Find().SetSort(bson.M{KEY_NAME: 1})
	var cur *mongo.Cursor
	ctx_tmp := ctx
	if s.inTransaction {
		ctx_tmp = s.sessCtx
	}
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
	cur, err = col.Find(ctx_tmp, condition, opts)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			fmt.Fprint(os.Stderr, "can't find the document\n")
			return nil
		}
		if s.inTransaction {
			_ = s.sessCtx.AbortTransaction(context.Background())
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

func (s *MongoDBKeyValueStore) PutWithCollection(ctx context.Context, kBytes []byte,
	vBytes []byte, collection string,
) error {
	if vBytes == nil {
		return s.DeleteWithCollection(ctx, kBytes, collection)
	} else {
		col := s.client.Database(s.config.DBName).Collection(collection)
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
		ctx_tmp := ctx
		if s.inTransaction {
			ctx_tmp = s.sessCtx
		}
		_, err := col.UpdateOne(ctx_tmp, bson.M{KEY_NAME: kBytes},
			bson.M{"$set": bson.M{VALUE_NAME: vBytes}}, opts)
		if err != nil {
			if s.inTransaction {
				err2 := s.sessCtx.AbortTransaction(context.Background())
				if err2 != nil {
					return fmt.Errorf("try to abort transaction while handling err (%v) and abort failed: %v", err, err2)
				}
			}
			return err
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
	col := s.client.Database(s.config.DBName).Collection(collection)
	ctx_tmp := ctx
	if s.inTransaction {
		ctx_tmp = s.sessCtx
	}
	_, err := col.DeleteOne(ctx_tmp, bson.M{KEY_NAME: kBytes})
	if err != nil {
		if s.inTransaction {
			_ = s.sessCtx.AbortTransaction(context.Background())
		}
		return err
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
