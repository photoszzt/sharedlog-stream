package store

import (
	"context"
	"sharedlog-stream/pkg/stream/processor/commtypes"

	"go.mongodb.org/mongo-driver/bson"
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

func NewMongoDBKeyValueStore(ctx context.Context, config *MongoDBConfig) (*MongoDBKeyValueStore, error) {
	clientOpts := options.Client().ApplyURI(config.Addr)
	client, err := mongo.Connect(ctx, clientOpts)
	if err != nil {
		return nil, err
	}
	return &MongoDBKeyValueStore{
		config: config,
		client: client,
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
	var result []byte
	if s.inTransaction {
		err = col.FindOne(s.sessCtx, bson.D{{"key", kBytes}}).Decode(&result)
		if err != nil {
			if err == mongo.ErrNoDocuments {
				return nil, false, nil
			}
			_ = s.sessCtx.AbortTransaction(context.Background())
			return nil, false, err
		}

	} else {
		err = col.FindOne(ctx, bson.D{{"key", kBytes}}).Decode(&result)
		if err != nil {
			if err == mongo.ErrNoDocuments {
				return nil, false, nil
			}
			return nil, false, err
		}
	}
	val, err := s.config.ValueSerde.Decode(result)
	if err != nil {
		return nil, false, err
	}
	return val, true, nil
}

func (s *MongoDBKeyValueStore) Range(ctx context.Context, from commtypes.KeyT, to commtypes.KeyT,
	iterFunc func(commtypes.KeyT, commtypes.ValueT) error,
) error {
	panic("not implemented")
}

func (s *MongoDBKeyValueStore) ReverseRange(from commtypes.KeyT, to commtypes.KeyT, iterFunc func(commtypes.KeyT, commtypes.ValueT) error) error {
	panic("not implemented")
}

func (s *MongoDBKeyValueStore) ApproximateNumEntriesWithCollection(
	ctx context.Context, collection_name string,
) (uint64, error) {
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
		// assume key and value to be bytes
		kBytes, ok := key.([]byte)
		var err error
		if !ok {
			kBytes, err = s.config.KeySerde.Encode(key)
			if err != nil {
				return err
			}
		}
		vBytes, ok := value.([]byte)
		if !ok {
			vBytes, err = s.config.ValueSerde.Encode(value)
			if err != nil {
				return err
			}
		}
		col := s.client.Database(s.config.DBName).Collection(collection)
		opts := options.Update().SetUpsert(true)
		if s.inTransaction {
			_, err = col.UpdateOne(s.sessCtx, bson.D{{"key", kBytes}}, bson.D{{"$set", bson.D{{"value", vBytes}}}}, opts)
			if err != nil {
				s.sessCtx.AbortTransaction(context.Background())
				return err
			}
		} else {
			_, err = col.UpdateOne(ctx, bson.D{{"key", kBytes}}, bson.D{{"$set", bson.D{{"value", vBytes}}}}, opts)
			if err != nil {
				return err
			}
		}
		return err
	}
}

func (s *MongoDBKeyValueStore) PutIfAbsent(ctx context.Context, key commtypes.KeyT, value commtypes.ValueT) (commtypes.ValueT, error) {
	// assume key and value to be bytes
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
	_ = kBytes
	return nil
}
