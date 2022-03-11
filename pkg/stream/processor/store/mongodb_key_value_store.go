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
	StoreName      string
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
	col := client.Database(config.DBName).Collection(config.CollectionName)
	idxView := col.Indexes()
	_, err = idxView.CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: "key", Value: 1}},
		Options: options.Index().SetName("kv"),
	})
	if err != nil {
		return nil, err
	}
	return &MongoDBKeyValueStore{
		config:        config,
		client:        client,
		indexCreation: make(map[string]struct{}),
		inTransaction: false,
	}, nil
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

func (s *MongoDBKeyValueStore) CommitTransaction(ctx context.Context) error {
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
	return s.config.StoreName
}

func (s *MongoDBKeyValueStore) Get(ctx context.Context, key commtypes.KeyT) (commtypes.ValueT, bool, error) {
	return s.GetWithCollection(ctx, key, s.config.CollectionName)
}

func (s *MongoDBKeyValueStore) GetWithCollection(ctx context.Context, key commtypes.KeyT, collection string) (commtypes.ValueT, bool, error) {
	var err error
	kBytes, ok := key.([]byte)
	if !ok {
		kBytes, err = s.config.KeySerde.Encode(key)
		if err != nil {
			return nil, false, err
		}
	}
	col := s.client.Database(s.config.DBName).Collection(collection)
	var result bson.M
	if s.inTransaction {
		err = col.FindOne(s.sessCtx, bson.D{{Key: "key", Value: kBytes}}).Decode(&result)
		if err != nil {
			if err == mongo.ErrNoDocuments {
				fmt.Fprint(os.Stderr, "can't find the document\n")
				return nil, false, nil
			}
			_ = s.sessCtx.AbortTransaction(context.Background())
			return nil, false, err
		}
	} else {
		err = col.FindOne(ctx, bson.D{{Key: "key", Value: kBytes}}).Decode(&result)
		if err != nil {
			if err == mongo.ErrNoDocuments {
				fmt.Fprint(os.Stderr, "can't find the document\n")
				return nil, false, nil
			}
			return nil, false, err
		}
	}
	valBytes := result["value"]
	if valBytes == nil {
		return nil, true, nil
	}
	val, err := s.config.ValueSerde.Decode(valBytes.(primitive.Binary).Data)
	if err != nil {
		return nil, false, err
	}
	return val, true, nil
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
	col := s.client.Database(s.config.DBName).Collection(s.config.CollectionName)
	opts := options.Find().SetSort(bson.D{{Key: "key", Value: 1}})
	var cur *mongo.Cursor
	ctx_tmp := ctx
	if s.inTransaction {
		ctx_tmp = s.sessCtx
	}
	var condition bson.D
	if from == nil && to == nil {
		condition = bson.D{}
	} else if from == nil && to != nil {
		condition = bson.D{{Key: "key", Value: bson.D{{Key: "$lte", Value: toBytes}}}}
	} else if from != nil && to == nil {
		condition = bson.D{{
			Key:   "key",
			Value: bson.D{{Key: "$gte", Value: fromBytes}}}}
	} else {
		condition = bson.D{{Key: "key",
			Value: bson.D{
				{Key: "$gte", Value: fromBytes},
				{Key: "$lte", Value: toBytes}}}}
	}
	fmt.Fprintf(os.Stderr, "condition: %v\n", condition)
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
	for cur.Next(ctx) {
		var res bson.M
		err = cur.Decode(&res)
		if err != nil {
			return err
		}
		kBytes := res["key"].(primitive.Binary).Data
		vBytes := res["value"].(primitive.Binary).Data
		k, err := s.config.KeySerde.Decode(kBytes)
		if err != nil {
			return err
		}
		v, err := s.config.ValueSerde.Decode(vBytes)
		if err != nil {
			return err
		}
		err = iterFunc(k, v)
		if err != nil {
			return err
		}
	}
	return nil
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
	return s.PutWithCollection(ctx, key, value, s.config.CollectionName)
}

func (s *MongoDBKeyValueStore) PutWithCollection(ctx context.Context, key commtypes.KeyT,
	value commtypes.ValueT, collection string,
) error {
	if value == nil {
		return s.Delete(ctx, key)
	} else {
		col := s.client.Database(s.config.DBName).Collection(collection)
		_, ok := s.indexCreation[collection]
		if !ok {
			idxView := col.Indexes()
			_, err := idxView.CreateOne(ctx, mongo.IndexModel{
				Keys:    bson.D{{Key: "key", Value: 1}},
				Options: options.Index().SetName("kv"),
			})
			if err != nil {
				return err
			}
			s.indexCreation[collection] = struct{}{}
		}
		// assume key and value to be bytes
		kBytes, err := utils.ConvertToBytes(key, s.config.KeySerde)
		if err != nil {
			return err
		}
		vBytes, err := utils.ConvertToBytes(value, s.config.ValueSerde)
		if err != nil {
			return err
		}
		fmt.Fprintf(os.Stderr, "put k: %v, val: %v\n", kBytes, vBytes)
		opts := options.Update().SetUpsert(true)
		ctx_tmp := ctx
		if s.inTransaction {
			ctx_tmp = s.sessCtx
		}
		_, err = col.UpdateOne(ctx_tmp, bson.D{{Key: "key", Value: kBytes}},
			bson.D{{Key: "$set", Value: bson.D{{Key: "value", Value: vBytes}}}}, opts)
		if err != nil {
			if s.inTransaction {
				s.sessCtx.AbortTransaction(context.Background())
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
	return s.DeleteWithCollection(ctx, key, s.config.CollectionName)
}

func (s *MongoDBKeyValueStore) DeleteWithCollection(ctx context.Context,
	key commtypes.KeyT, collection string,
) error {
	kBytes, err := s.config.KeySerde.Encode(key)
	if err != nil {
		return err
	}
	col := s.client.Database(s.config.DBName).Collection(collection)
	ctx_tmp := ctx
	if s.inTransaction {
		ctx_tmp = s.sessCtx
	}
	_, err = col.DeleteOne(ctx_tmp, bson.D{{Key: "key", Value: kBytes}})
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
