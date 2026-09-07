//
//  Copyright 2026 The InfiniFlow Authors. All Rights Reserved.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//

package nats

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"ragflow/internal/common"

	"github.com/nats-io/nats.go/jetstream"
)

// TestPullTaskStreamDeliversFirstMessageBeforeBatchCompletes proves the
// dispatcher can hand a task to an idle slot without waiting for a requested
// batch to fill. Replacing PullTaskStream with the old slice-collecting Fetch
// path would make this test time out.
func TestPullTaskStreamDeliversFirstMessageBeforeBatchCompletes(t *testing.T) {
	host, port := newEmbeddedNatsServer(t)
	queue := NewNatsEngine(host, port)
	if err := queue.Init(); err != nil {
		t.Fatalf("Init: %v", err)
	}
	if err := queue.InitConsumer(common.TaskSubject); err != nil {
		t.Fatalf("InitConsumer: %v", err)
	}

	payload, err := json.Marshal(common.TaskMessage{
		TaskID:   "stream-first-message",
		TaskType: common.TaskTypeIngestionTask,
	})
	if err != nil {
		t.Fatalf("marshal task message: %v", err)
	}
	if err := queue.PublishTask(common.TaskSubject, payload); err != nil {
		t.Fatalf("PublishTask: %v", err)
	}

	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	stream, err := queue.PullTaskStream(ctx, 2)
	if err != nil {
		t.Fatalf("PullTaskStream: %v", err)
	}

	select {
	case handle := <-stream.Messages():
		if handle == nil {
			t.Fatal("PullTaskStream closed before delivering the available message")
		}
		if got := handle.GetMessage().TaskID; got != "stream-first-message" {
			t.Fatalf("task id = %q, want %q", got, "stream-first-message")
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatal("first task was delayed until the requested batch completed")
	}
}

// TestPullTaskStreamRequiresDeadline prevents an unbounded server-side pending
// request when a caller later cancels its local context.
func TestPullTaskStreamRequiresDeadline(t *testing.T) {
	host, port := newEmbeddedNatsServer(t)
	queue := NewNatsEngine(host, port)
	if err := queue.Init(); err != nil {
		t.Fatalf("Init: %v", err)
	}
	if err := queue.InitConsumer(common.TaskSubject); err != nil {
		t.Fatalf("InitConsumer: %v", err)
	}

	_, err := queue.PullTaskStream(t.Context(), 1)
	if err == nil {
		t.Fatal("PullTaskStream without a deadline succeeded")
	}
	if !strings.Contains(err.Error(), "deadline") {
		t.Fatalf("PullTaskStream error = %v, want deadline error", err)
	}
}

// TestPullTaskStreamCancellationEndsLocalStream proves callers can stop a
// pending Pull without waiting for its server-side expiry. The broker request
// may persist until the deadline supplied to FetchContext, but it must not
// survive that bounded window.
func TestPullTaskStreamCancellationEndsLocalStream(t *testing.T) {
	host, port := newEmbeddedNatsServer(t)
	queue := NewNatsEngine(host, port)
	if err := queue.Init(); err != nil {
		t.Fatalf("Init: %v", err)
	}
	if err := queue.InitConsumer(common.TaskSubject); err != nil {
		t.Fatalf("InitConsumer: %v", err)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 150*time.Millisecond)
	stream, err := queue.PullTaskStream(ctx, 1)
	if err != nil {
		cancel()
		t.Fatalf("PullTaskStream: %v", err)
	}
	cancel()

	select {
	case <-stream.Done():
	case <-time.After(250 * time.Millisecond):
		t.Fatal("cancelled PullTaskStream did not end promptly")
	}
	if !errors.Is(stream.Err(), context.Canceled) {
		t.Fatalf("PullTaskStream error = %v, want context cancellation", stream.Err())
	}

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		info, err := queue.consumer.Info(t.Context())
		if err != nil {
			t.Fatalf("consumer info: %v", err)
		}
		if info.NumWaiting == 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("cancelled PullTaskStream remained pending beyond its request expiry")
}

// TestPullTaskStreamReportsConnectionClose ensures a broker connection loss is
// observable as a Pull failure rather than being confused with an empty queue
// reaching its request expiry.
func TestPullTaskStreamReportsConnectionClose(t *testing.T) {
	host, port := newEmbeddedNatsServer(t)
	queue := NewNatsEngine(host, port)
	if err := queue.Init(); err != nil {
		t.Fatalf("Init: %v", err)
	}
	if err := queue.InitConsumer(common.TaskSubject); err != nil {
		t.Fatalf("InitConsumer: %v", err)
	}

	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	stream, err := queue.PullTaskStream(ctx, 1)
	if err != nil {
		t.Fatalf("PullTaskStream: %v", err)
	}
	queue.nc.Close()

	select {
	case <-stream.Done():
	case <-time.After(250 * time.Millisecond):
		t.Fatal("connection close did not end pending PullTaskStream")
	}
	if err := stream.Err(); !errors.Is(err, errTaskStreamConnectionLost) {
		t.Fatalf("connection-close error = %v, want connection-lost failure", err)
	}
}

// TestPullTaskStreamReportsMaxWaiting distinguishes a consumer capacity
// rejection from a request that simply reached its normal expiry. The
// dispatcher can retry a rejected slot without treating it as an empty queue.
func TestPullTaskStreamReportsMaxWaiting(t *testing.T) {
	host, port := newEmbeddedNatsServer(t)
	queue := NewNatsEngine(host, port)
	if err := queue.Init(); err != nil {
		t.Fatalf("Init: %v", err)
	}
	if err := queue.InitConsumer(common.TaskSubject); err != nil {
		t.Fatalf("InitConsumer: %v", err)
	}

	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	if err := queue.stream.DeleteConsumer(ctx, "RAGFLOW_CONSUMER"); err != nil {
		t.Fatalf("delete default consumer: %v", err)
	}
	consumer, err := queue.stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Name:          "RAGFLOW_CONSUMER",
		AckPolicy:     jetstream.AckExplicitPolicy,
		FilterSubject: "tasks.>",
		MaxWaiting:    2,
	})
	if err != nil {
		t.Fatalf("create limited consumer: %v", err)
	}
	queue.consumer = consumer

	firstCtx, cancelFirst := context.WithTimeout(t.Context(), time.Second)
	defer cancelFirst()
	secondCtx, cancelSecond := context.WithTimeout(t.Context(), time.Second)
	defer cancelSecond()
	if _, err := queue.PullTaskStream(firstCtx, 1); err != nil {
		t.Fatalf("first PullTaskStream: %v", err)
	}
	if _, err := queue.PullTaskStream(secondCtx, 1); err != nil {
		t.Fatalf("second PullTaskStream: %v", err)
	}

	deadline := time.Now().Add(time.Second)
	waitingReached := false
	for time.Now().Before(deadline) {
		info, err := queue.consumer.Info(t.Context())
		if err != nil {
			t.Fatalf("consumer info: %v", err)
		}
		if info.NumWaiting == 2 {
			waitingReached = true
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !waitingReached {
		t.Fatal("first two PullTaskStreams did not occupy the configured waiting capacity")
	}

	thirdCtx, cancelThird := context.WithTimeout(t.Context(), time.Second)
	defer cancelThird()
	stream, err := queue.PullTaskStream(thirdCtx, 1)
	if err == nil {
		select {
		case <-stream.Done():
			err = stream.Err()
		case <-time.After(250 * time.Millisecond):
			t.Fatal("MaxWaiting rejection did not end the PullTaskStream")
		}
	}
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("MaxWaiting error = %v, want a distinct capacity rejection", err)
	}
}

// TestPullMessagesForAdminLimitsConcurrentRequests prevents manual pulls from
// consuming every MaxWaiting slot that the dispatcher needs. The default admin
// quota admits one request and rejects the next without waiting for Fetch's
// one-second expiry.
func TestPullMessagesForAdminLimitsConcurrentRequests(t *testing.T) {
	host, port := newEmbeddedNatsServer(t)
	queue := NewNatsEngine(host, port)
	if err := queue.Init(); err != nil {
		t.Fatalf("Init: %v", err)
	}
	if err := queue.InitConsumer(common.TaskSubject); err != nil {
		t.Fatalf("InitConsumer: %v", err)
	}

	firstDone := make(chan error, 1)
	go func() {
		_, err := queue.PullMessagesForAdmin(1)
		firstDone <- err
	}()
	t.Cleanup(func() { <-firstDone })

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		info, err := queue.consumer.Info(t.Context())
		if err != nil {
			t.Fatalf("consumer info: %v", err)
		}
		if info.NumWaiting == 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	secondDone := make(chan error, 1)
	go func() {
		_, err := queue.PullMessagesForAdmin(1)
		secondDone <- err
	}()
	select {
	case err := <-secondDone:
		if err == nil {
			t.Fatal("second concurrent admin Pull succeeded, want quota rejection")
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatal("second concurrent admin Pull waited for the first Fetch to expire")
	}
}
