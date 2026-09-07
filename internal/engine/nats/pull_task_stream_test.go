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
	"strings"
	"testing"
	"time"

	"ragflow/internal/common"
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

// TestValidateTaskPullCapacityRejectsInsufficientMaxWaiting prevents startup
// from accepting a dispatcher that the durable consumer cannot serve.
func TestValidateTaskPullCapacityRejectsInsufficientMaxWaiting(t *testing.T) {
	host, port := newEmbeddedNatsServer(t)
	queue := NewNatsEngine(host, port)
	if err := queue.Init(); err != nil {
		t.Fatalf("Init: %v", err)
	}
	if err := queue.InitConsumer(common.TaskSubject); err != nil {
		t.Fatalf("InitConsumer: %v", err)
	}

	if err := queue.ValidateTaskPullCapacity(513); err == nil {
		t.Fatal("ValidateTaskPullCapacity accepted a requirement above the consumer MaxWaiting")
	}
}
