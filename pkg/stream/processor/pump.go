package processor

import (
	"context"
	"sharedlog-stream/pkg/commtypes"
	"sync"
)

type Pump interface {
	sync.Locker

	Accept(context.Context, commtypes.Message) error
	Stop()
	Close() error
}

type syncPump struct {
	sync.Mutex
	processor Processor
	pipe      Pipe
	name      string
}

func NewSyncPump(node Node, pipe Pipe) Pump {
	p := &syncPump{
		name:      node.Name(),
		processor: node.Processor(),
		pipe:      pipe,
	}

	return p
}

func (p *syncPump) Accept(ctx context.Context, msg commtypes.Message) error {
	err := p.processor.Process(ctx, msg)
	if err != nil {
		return err
	}
	return nil
}

func (p *syncPump) Stop() {}

func (p *syncPump) Close() error {
	return nil
}

/*
type asyncPump struct {
	sync.Mutex

	name string,
	processor Processor,
	pipe Pipe,
	errFn ErrorFunc

	ch chan commtypes.Message
	wg sync.WaitGroup
}

func NewAsyncPump(node Node, pipe Pipe, errFn ErrorFunc) Pump {
	p := &asyncPump {
		name: node.Name(),
		processor: node.Processor(),
		pipe: pipe,
		errFn: errFn,
		ch: make(chan commtypes.Message),
	}
}

func (p *asyncPump) run() {
	p.wg.Add(1)
	defer p.wg.Done()

	for msg := range p.ch {
		p.Lock()

		err := p.processor.Process(msg)
		if err != nil {
			p.Unlock()
			p.errFn(err)
			return
		}

		p.Unlock()
	}
}


type SourcePump interface {
	Stop()

	Close() error
}

type sourcePump struct {
	wg     sync.WaitGroup
	source Source
	errFn  ErrorFunc
	quit   chan struct{}
	name   string
	pumps  []Pump
	parNum uint8
	ctx    context.Context
}

func NewSourcePump(ctx context.Context, name string, source Source, parNum uint8, pumps []Pump, errFn ErrorFunc) SourcePump {
	p := &sourcePump{
		name:   name,
		source: source,
		pumps:  pumps,
		errFn:  errFn,
		quit:   make(chan struct{}, 2),
		parNum: parNum,
		ctx:    ctx,
	}
	go p.run()
	return p
}

func (p *sourcePump) run() {
	p.wg.Add(1)
	defer p.wg.Done()

	for {
		select {
		case <-p.quit:
			return
		default:
			msgs, err := p.source.Consume(p.ctx, p.parNum)
			if err != nil {
				go p.errFn(err)
				return
			}

			for _, msg := range msgs.Msgs {
				if msg.Msg.Value == nil {
					continue
				}

				for _, pump := range p.pumps {
					err = pump.Accept(p.ctx, msg.Msg)
					if err != nil {
						go p.errFn(err)
						return
					}
				}
			}
		}
	}
}

func (p *sourcePump) Stop() {
	p.quit <- struct{}{}

	p.wg.Wait()
}

func (p *sourcePump) Close() error {
	close(p.quit)
	return nil
}
*/
