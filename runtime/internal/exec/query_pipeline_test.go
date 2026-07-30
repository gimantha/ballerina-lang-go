// Copyright (c) 2026, WSO2 LLC. (http://www.wso2.com).
//
// WSO2 LLC. licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except
// in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package exec

import (
	"testing"

	"ballerina/values"
)

type querySliceIterator struct {
	values     []values.BalValue
	completion values.BalValue
	index      int
	pulls      int
	closed     bool
}

func (i *querySliceIterator) pull() (values.BalValue, values.BalValue, bool, bool) {
	i.pulls++
	if i.index >= len(i.values) {
		return nil, i.completion, false, true
	}
	value := i.values[i.index]
	i.index++
	return value, nil, true, false
}

func (i *querySliceIterator) close() values.BalValue {
	i.closed = true
	return nil
}

func TestQueryPipelinePullsRowsLazily(t *testing.T) {
	iterator := &querySliceIterator{
		values: []values.BalValue{int64(1), int64(2), int64(3), int64(4)},
	}
	var stage queryStage = newQueryFromStage(
		&querySingletonStage{},
		func(queryRow) queryIterator {
			return iterator
		},
	)
	stage = newQueryFilterStage(stage, func(row queryRow) queryEvalResult {
		return queryEvalResult{value: row[0].(int64)%2 == 0}
	})
	stage = newQueryMapStage(stage, []queryEvaluator{
		func(row queryRow) queryEvalResult {
			return queryEvalResult{value: row[0].(int64) * 10}
		},
	})
	stage = newQueryLimitStage(stage, 1)
	stage = newQuerySelectStage(stage, func(row queryRow) queryEvalResult {
		return queryEvalResult{value: row[1]}
	})

	result := stage.pull()
	if !result.hasRow || len(result.row) != 1 || result.row[0] != int64(20) {
		t.Fatalf("expected first selected value to be 20, got %#v", result)
	}
	if iterator.pulls != 2 {
		t.Fatalf("expected source to be pulled only through the first matching row, got %d pulls", iterator.pulls)
	}

	result = stage.pull()
	if result.hasRow || result.completion != nil {
		t.Fatalf("expected normal completion after limit, got %#v", result)
	}
	if iterator.pulls != 2 {
		t.Fatalf("expected completed limit not to pull its source, got %d pulls", iterator.pulls)
	}
}

func TestQueryPipelineSupportsNestedFromStages(t *testing.T) {
	var stage queryStage = newQueryFromStage(
		&querySingletonStage{},
		func(queryRow) queryIterator {
			return &querySliceIterator{values: []values.BalValue{int64(1), int64(2)}}
		},
	)
	stage = newQueryFromStage(stage, func(queryRow) queryIterator {
		return &querySliceIterator{values: []values.BalValue{int64(10), int64(20)}}
	})

	expected := []queryRow{
		{int64(1), int64(10)},
		{int64(1), int64(20)},
		{int64(2), int64(10)},
		{int64(2), int64(20)},
	}
	for index, expectedRow := range expected {
		result := stage.pull()
		if !result.hasRow {
			t.Fatalf("expected row %d, got completion %#v", index, result.completion)
		}
		if len(result.row) != len(expectedRow) {
			t.Fatalf("expected row %d to have %d values, got %#v", index, len(expectedRow), result.row)
		}
		for valueIndex := range expectedRow {
			if result.row[valueIndex] != expectedRow[valueIndex] {
				t.Fatalf("expected row %d to be %#v, got %#v", index, expectedRow, result.row)
			}
		}
	}
	if result := stage.pull(); result.hasRow || result.completion != nil {
		t.Fatalf("expected nested from pipeline to complete normally, got %#v", result)
	}
}

func TestQueryPipelinePropagatesIteratorCompletion(t *testing.T) {
	completion := values.NewErrorWithMessage("source failed")
	stage := newQueryFromStage(
		&querySingletonStage{},
		func(queryRow) queryIterator {
			return &querySliceIterator{
				values:     []values.BalValue{int64(1)},
				completion: completion,
			}
		},
	)

	if result := stage.pull(); !result.hasRow || result.row[0] != int64(1) {
		t.Fatalf("expected source value before completion, got %#v", result)
	}
	if result := stage.pull(); result.hasRow || result.completion != completion {
		t.Fatalf("expected source completion to propagate, got %#v", result)
	}
	if result := stage.pull(); result.hasRow || result.completion != completion {
		t.Fatalf("expected terminal completion to remain stable, got %#v", result)
	}
}

func TestQueryPipelineResumesAfterEvaluatorCompletion(t *testing.T) {
	completion := values.NewErrorWithMessage("checked completion")
	var stage queryStage = newQueryFromStage(
		&querySingletonStage{},
		func(queryRow) queryIterator {
			return &querySliceIterator{values: []values.BalValue{int64(1), int64(2)}}
		},
	)
	stage = newQuerySelectStage(stage, func(row queryRow) queryEvalResult {
		if row[0] == int64(1) {
			return queryEvalResult{completion: completion}
		}
		return queryEvalResult{value: row[0]}
	})

	first := stage.pull()
	if first.hasRow || first.terminal || first.completion != completion {
		t.Fatalf("expected transient evaluator completion, got %#v", first)
	}
	second := stage.pull()
	if !second.hasRow || second.row[0] != int64(2) {
		t.Fatalf("expected pipeline to resume at the next row, got %#v", second)
	}
	if result := stage.pull(); result.hasRow || !result.terminal || result.completion != nil {
		t.Fatalf("expected normal terminal completion, got %#v", result)
	}
}
