package store

import (
	"context"
	"os"
	"sharedlog-stream/pkg/debug"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readconcern"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"
)

func InitMongoDBClient(ctx context.Context, addr string) (*mongo.Client, error) {
	clientOpts := options.Client().ApplyURI(addr).
		SetReadConcern(readconcern.Linearizable()).
		SetWriteConcern(writeconcern.New(writeconcern.WMajority()))
	return mongo.Connect(ctx, clientOpts)
}

func RunTranFuncWithRetry(sctx mongo.SessionContext, txnFn func(mongo.SessionContext) error) error {
	for {
		err := txnFn(sctx)
		if err == nil {
			return nil
		}
		if cmdErr, ok := err.(mongo.CommandError); ok && cmdErr.HasErrorLabel("TransientTransactionError") {
			time.Sleep(time.Duration(10) * time.Millisecond)
			continue
		}
		return err
	}
}

func CommitWithRetry(sctx mongo.SessionContext) error {
	for {
		err := sctx.CommitTransaction(sctx)
		switch e := err.(type) {
		case nil:
			return nil
		case mongo.CommandError:
			if e.HasErrorLabel("UnknownTransactionCommitResult") {
				time.Sleep(time.Duration(10) * time.Millisecond)
				continue
			}
			if e.HasErrorLabel("TransientTransactionError") {
				_ = sctx.AbortTransaction(context.Background())
			}
			debug.Fprintf(os.Stderr, "commit error is %v, code %d, labels %v\n", e, e.Code, e.Labels)
			return e
		default:
			return e
		}
	}
}
