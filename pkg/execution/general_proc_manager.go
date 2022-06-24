package execution

import (
	"context"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/processor"
	"sharedlog-stream/pkg/producer_consumer"
	"sync"
)

type GeneralProcManager struct {
	msgChan         chan commtypes.Message
	errChan         chan error
	generalProcFunc GeneralProcFunc
}

type GeneralProcFunc func(ctx context.Context,
	args interface{}, wg *sync.WaitGroup,
	msgChan chan commtypes.Message, errChan chan error)

func NewGeneralProcManager(generalProcFunc GeneralProcFunc) *GeneralProcManager {
	return &GeneralProcManager{
		msgChan:         make(chan commtypes.Message),
		errChan:         make(chan error, 1),
		generalProcFunc: generalProcFunc,
	}
}

func (gm *GeneralProcManager) MsgChan() chan commtypes.Message {
	return gm.msgChan
}

func (gm *GeneralProcManager) ErrChan() chan error {
	return gm.errChan
}

func (gm *GeneralProcManager) LaunchProc(ctx context.Context, args interface{}, wg *sync.WaitGroup) {
	wg.Add(1)
	go gm.generalProcFunc(ctx, args, wg, gm.msgChan, gm.errChan)
}

func (gm *GeneralProcManager) RequestToTerminate() {
	close(gm.msgChan)
}

func (gm *GeneralProcManager) RecreateMsgChan(updateMsg *chan commtypes.Message) {
	gm.msgChan = make(chan commtypes.Message, 1)
	*updateMsg = gm.msgChan
}

type GeneralProcCtx struct {
	chains processor.ProcessorChains
}

func NewGeneralProcCtx() *GeneralProcCtx {
	return &GeneralProcCtx{
		chains: processor.NewProcessorChains(),
	}
}

func (c *GeneralProcCtx) AppendProcessor(processor processor.Processor) {
	_ = c.chains.Via(processor)
}

func (c *GeneralProcCtx) GeneralProc(ctx context.Context,
	producer producer_consumer.Producer,
	msgChan chan commtypes.Message,
	errChan chan error,
) {
L:
	for {
		producer.Lock()
		select {
		case <-ctx.Done():
			producer.Unlock()
			break L
		case msg, ok := <-msgChan:
			if !ok {
				producer.Unlock()
				break L
			}
			_, err := c.chains.RunChains(ctx, msg)
			if err != nil {
				errChan <- err
				producer.Unlock()
				return
			}
		default:
		}
		producer.Unlock()
	}
}
