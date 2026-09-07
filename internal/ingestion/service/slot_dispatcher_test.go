//
// Copyright 2026 The InfiniFlow Authors. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

package service

import (
	"context"
	"sync"
	"testing"
	"time"

	"ragflow/internal/common"
	"ragflow/internal/engine"
)

type blockingTaskHandleStream struct {
	messages chan common.TaskHandle
	done     chan struct{}

	once sync.Once
	mu   sync.RWMutex
	err  error
}

func newBlockingTaskHandleStream() *blockingTaskHandleStream {
	return &blockingTaskHandleStream{
		messages: make(chan common.TaskHandle),
		done:     make(chan struct{}),
	}
}

func (s *blockingTaskHandleStream) Messages() <-chan common.TaskHandle { return s.messages }

func (s *blockingTaskHandleStream) Done() <-chan struct{} { return s.done }

func (s *blockingTaskHandleStream) Err() error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.err
}

func (s *blockingTaskHandleStream) close(err error) {
	s.once.Do(func() {
		s.mu.Lock()
		s.err = err
		s.mu.Unlock()
		close(s.messages)
		close(s.done)
	})
}

type slotTestQueue struct {
	engine.MessageQueue

	mu      sync.Mutex
	streams []*blockingTaskHandleStream
	next    int
	calls   chan int
}

func (q *slotTestQueue) PullTaskStream(ctx context.Context, max int) (common.TaskHandleStream, error) {
	q.mu.Lock()
	stream := q.streams[q.next]
	q.next++
	q.mu.Unlock()

	q.calls <- max
	go func() {
		<-ctx.Done()
		stream.close(ctx.Err())
	}()
	return stream, nil
}

// TestSlotDispatcherStartsNewPullWhileEarlierPullWaits prevents a pending
// Pull(1) from delaying a newly-idle worker until its one-second expiry. A
// synchronous Fetch loop would only record the first call before the timeout.
func TestSlotDispatcherStartsNewPullWhileEarlierPullWaits(t *testing.T) {
	queue := &slotTestQueue{
		streams: []*blockingTaskHandleStream{
			newBlockingTaskHandleStream(),
			newBlockingTaskHandleStream(),
		},
		calls: make(chan int, 2),
	}
	previousQueue := engine.GetMessageQueueEngine()
	engine.SetMessageQueueEngine(queue)
	t.Cleanup(func() { engine.SetMessageQueueEngine(previousQueue) })

	ingestor := newUnitIngestor("test-independent-pulls", 2, nil)
	ingestor.dispatcherWg.Add(1)
	go ingestor.consumeLoop()
	t.Cleanup(func() {
		ingestor.dispatchCancel()
		ingestor.dispatcherWg.Wait()
		ingestor.pullWg.Wait()
	})

	ingestor.idleSlots <- &workerSlot{id: 1, inbox: make(chan common.TaskHandle)}
	select {
	case max := <-queue.calls:
		if max != 1 {
			t.Fatalf("first Pull max = %d, want 1", max)
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatal("first idle slot did not start Pull(1)")
	}

	ingestor.idleSlots <- &workerSlot{id: 2, inbox: make(chan common.TaskHandle)}
	select {
	case max := <-queue.calls:
		if max != 1 {
			t.Fatalf("second Pull max = %d, want 1", max)
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatal("new idle slot waited for the earlier Pull to expire")
	}
}
