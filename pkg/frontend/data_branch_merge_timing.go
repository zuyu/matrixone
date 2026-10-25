// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package frontend

import (
	"time"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"go.uber.org/zap"
)

// Timings belong to the serial merge owner, including its synchronous apply
// callbacks. They measure wall time, not CPU time. Request phases include child
// table phases; consumers must not sum request and table events together.
type dataBranchMergeTiming struct {
	requestID, scope, source, destination string
	transactionMode, finishAction         string
	started                               time.Time
	now                                   func() time.Time
	phases                                map[string]time.Duration
	parent                                *dataBranchMergeTiming
	tables                                int
}

func newDataBranchMergeTiming(scope string, source, destination tree.TableName, ses *Session) *dataBranchMergeTiming {
	t := &dataBranchMergeTiming{
		requestID: uuid.NewString(), scope: scope,
		source:      tree.String(&source, dialect.MYSQL),
		destination: tree.String(&destination, dialect.MYSQL),
		now:         time.Now, started: time.Now(), phases: make(map[string]time.Duration),
		transactionMode: "owned", finishAction: "not_acquired",
	}
	if op := ses.proc.GetTxnOperator(); op != nil && op.TxnOptions().ByBegin {
		t.transactionMode = "explicit"
	}
	return t
}

func (t *dataBranchMergeTiming) table(source, destination tree.TableName) *dataBranchMergeTiming {
	if t == nil {
		return nil
	}
	return &dataBranchMergeTiming{
		requestID: t.requestID, scope: "table",
		source:      tree.String(&source, dialect.MYSQL),
		destination: tree.String(&destination, dialect.MYSQL),
		now:         t.now, started: t.now(), phases: make(map[string]time.Duration), parent: t,
	}
}

func (t *dataBranchMergeTiming) measure(phase string, fn func() error) error {
	if t == nil {
		return fn()
	}
	start := t.now()
	defer func() { t.phases[phase] += t.now().Sub(start) }()
	return fn()
}

// A first flush can prepare its stage inside apply. Charge that DDL only to
// stage_init, including when it fails or panics, to keep exclusive wall times.
func (t *dataBranchMergeTiming) apply(fn func() error) error {
	if t == nil {
		return fn()
	}
	start, stageBefore := t.now(), t.phases["stage_init"]
	defer func() {
		t.phases["apply"] += max(time.Duration(0), t.now().Sub(start)-(t.phases["stage_init"]-stageBefore))
	}()
	return fn()
}

// The producer includes synchronous callbacks that initialize stages and apply
// rows. Subtract their complete duration so diff does not count that work again.
func (t *dataBranchMergeTiming) produce(produce func(emitFunc) error, emit emitFunc) error {
	if t == nil {
		return produce(emit)
	}
	start := t.now()
	var callbacks time.Duration
	defer func() { t.phases["diff"] += max(time.Duration(0), t.now().Sub(start)-callbacks) }()
	return produce(func(w batchWithKind) (bool, error) {
		start := t.now()
		defer func() { callbacks += t.now().Sub(start) }()
		return emit(w)
	})
}

// Wrap the callback, not finishDataBranchComponent: that function must remain
// directly deferred to recover panics and roll back the owner's transaction.
func (t *dataBranchMergeTiming) wrapFinish(finish func(error) error) func(error) error {
	if t == nil || finish == nil {
		return finish
	}
	return func(err error) error {
		t.finishAction = "commit"
		if err != nil {
			t.finishAction = "rollback"
		}
		if t.transactionMode == "explicit" {
			t.finishAction = "statement"
		}
		return t.measure("txn_finish", func() error { return finish(err) })
	}
}

func (t *dataBranchMergeTiming) account(total time.Duration) map[string]float64 {
	var measured time.Duration
	for _, duration := range t.phases {
		measured += duration
	}
	t.phases["other"] += max(time.Duration(0), total-measured)
	phases := make(map[string]float64)
	for _, name := range []string{"executor_acquire", "enumerate", "authorize", "relations",
		"prepare_worker", "lineage", "reconcile_schema", "diff", "stage_init", "apply",
		"stage_cleanup", "summary_output", "txn_finish", "other"} {
		phases[name] = float64(t.phases[name]) / float64(time.Millisecond)
	}
	if t.parent != nil {
		for phase, duration := range t.phases {
			t.parent.phases[phase] += duration
		}
		t.parent.tables++
	}
	return phases
}

// Register before resource/transaction defers so total includes their cleanup.
// A table may succeed before its request fails (e.g. a later table or COMMIT).
func (t *dataBranchMergeTiming) finish(err *error) {
	if t == nil {
		return
	}
	p := recover()
	status := "ok"
	if *err != nil {
		status = "error"
	}
	if p != nil {
		status = "panic"
	}
	total := t.now().Sub(t.started)
	fields := []zap.Field{
		zap.Int("schema_version", 1), zap.String("request_id", t.requestID),
		zap.String("scope", t.scope), zap.String("source", t.source),
		zap.String("destination", t.destination), zap.String("status", status),
		zap.Float64("total_ms", float64(total)/float64(time.Millisecond)),
		zap.Any("phases_ms", t.account(total)),
	}
	event := "DataBranch-Merge-Table-Timing"
	if t.parent == nil {
		event = "DataBranch-Merge-Request-Timing"
		fields = append(fields, zap.Int("table_count", t.tables),
			zap.String("transaction_mode", t.transactionMode), zap.String("finish_action", t.finishAction))
	}
	logutil.Info(event, fields...)
	if p != nil {
		panic(p)
	}
}
