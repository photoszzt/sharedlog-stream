package stream

import (
	"sync"
)

type Pump interface {
	sync.Locker

	Accept(Message) error
	Stop()
	Close() error
}

type syncPump struct {
	sync.Mutex

	name      string
	processor Processor
	pipe      Pipe
}

func NewSyncPump(node Node, pipe Pipe) Pump {
	p := &syncPump{
		name:      node.Name(),
		processor: node.Processor(),
		pipe:      pipe,
	}

	return p
}

func (p *syncPump) Accept(msg Message) error {
	err := p.processor.Process(msg)
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

	ch chan Message
	wg sync.WaitGroup
}

func NewAsyncPump(node Node, pipe Pipe, errFn ErrorFunc) Pump {
	p := &asyncPump {
		name: node.Name(),
		processor: node.Processor(),
		pipe: pipe,
		errFn: errFn,
		ch: make(chan Message),
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
*/

type SourcePump interface {
	Stop()

	Close() error
}

type sourcePump struct {
	name   string
	source Source
	pumps  []Pump
	errFn  ErrorFunc
	quit   chan struct{}
	wg     sync.WaitGroup
}

func NewSourcePump(name string, source Source, pumps []Pump, errFn ErrorFunc) SourcePump {
	p := &sourcePump{
		name:   name,
		source: source,
		pumps:  pumps,
		errFn:  errFn,
		quit:   make(chan struct{}, 2),
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
			msg, err := p.source.Consume()
			if err != nil {
				go p.errFn(err)
				return
			}

			if msg.Value == nil {
				continue
			}

			for _, pump := range p.pumps {
				err = pump.Accept(msg)
				if err != nil {
					go p.errFn(err)
					return
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
