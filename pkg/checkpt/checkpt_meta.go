package checkpt

import (
	"context"
	"sharedlog-stream/pkg/redis_client"
	"strconv"
	"time"

	"github.com/go-redis/redis/v9"
)

const (
	CHKPT_META_NODE   = 0
	REQ_CHKMNGR_ENDED = "req_chkmngr_ended"
	CHKPT_MNGR_ENDED  = "stream_ended"
)

type RedisChkptManager struct {
	rds []*redis.Client
}

func NewRedisChkptManager(ctx context.Context) (RedisChkptManager, error) {
	rcm := RedisChkptManager{
		rds: redis_client.GetRedisClients(),
	}
	var err error
	for {
		err := rcm.rds[CHKPT_META_NODE].Ping(ctx).Err()
		if err == nil {
			break
		}
		time.Sleep(time.Duration(100) * time.Millisecond)
	}
	err = rcm.rds[CHKPT_META_NODE].Set(ctx, REQ_CHKMNGR_ENDED, 0, 0).Err()
	if err != nil {
		return RedisChkptManager{}, err
	}
	err = rcm.rds[CHKPT_META_NODE].Set(ctx, CHKPT_MNGR_ENDED, 0, 0).Err()
	if err != nil {
		return RedisChkptManager{}, err
	}
	return rcm, nil
}

func (c *RedisChkptManager) ResetCheckPointCount(ctx context.Context, finalOutputTopicNames []string) error {
	_, err := c.rds[CHKPT_META_NODE].Pipelined(ctx, func(pipe redis.Pipeliner) error {
		for _, tp := range finalOutputTopicNames {
			pipe.Set(ctx, tp, 0, 0)
		}
		return nil
	})
	return err
}

func (c *RedisChkptManager) WaitForChkptFinish(
	ctx context.Context,
	finalOutputTopicNames []string,
	finalNumOutPartition []uint8,
) error {
	for {
		allFinished := true
		cmds, err := c.rds[CHKPT_META_NODE].Pipelined(ctx, func(pipe redis.Pipeliner) error {
			for _, tp := range finalOutputTopicNames {
				pipe.Get(ctx, tp)
			}
			return nil
		})
		if err != nil {
			return err
		}
		for idx, cmd := range cmds {
			parNum := finalNumOutPartition[idx]
			gotParStr := cmd.(*redis.StringCmd).Val()
			gotPar, err := strconv.Atoi(gotParStr)
			if err != nil {
				return err
			}
			if gotPar != int(parNum) {
				allFinished = false
				break
			}
		}
		if allFinished {
			return nil
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func (c *RedisChkptManager) FinishChkpt(ctx context.Context, finalOutParNames []string) error {
	_, err := c.rds[CHKPT_META_NODE].Pipelined(ctx, func(pipe redis.Pipeliner) error {
		for _, tp := range finalOutParNames {
			pipe.Incr(ctx, tp)
		}
		return nil
	})
	return err
}

func (c *RedisChkptManager) ReqChkMngrEnd(ctx context.Context) error {
	return c.rds[CHKPT_META_NODE].Set(ctx, REQ_CHKMNGR_ENDED, 1, 0).Err()
}

func (c *RedisChkptManager) GetReqChkMngrEnd(ctx context.Context) (int, error) {
	return c.rds[CHKPT_META_NODE].Get(ctx, REQ_CHKMNGR_ENDED).Int()
}

func (c *RedisChkptManager) SetChkMngrEnded(ctx context.Context) error {
	return c.rds[CHKPT_META_NODE].Set(ctx, CHKPT_MNGR_ENDED, 1, 0).Err()
}

func (c *RedisChkptManager) GetChkMngrEnded(ctx context.Context) (int, error) {
	return c.rds[CHKPT_META_NODE].Get(ctx, CHKPT_MNGR_ENDED).Int()
}
