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

import "ballerina/values"

type queryRow []values.BalValue

type queryPullResult struct {
	row        queryRow
	completion values.BalValue
	hasRow     bool
}

type queryStage interface {
	pull() queryPullResult
	close() values.BalValue
}

type queryIterator interface {
	pull() (values.BalValue, values.BalValue, bool)
	close() values.BalValue
}

type queryIteratorFactory func(queryRow) queryIterator

type queryEvalResult struct {
	value      values.BalValue
	completion values.BalValue
}

type queryEvaluator func(queryRow) queryEvalResult

type querySingletonStage struct {
	pulled bool
	closed bool
}

func (s *querySingletonStage) pull() queryPullResult {
	if s.pulled || s.closed {
		return queryPullResult{}
	}
	s.pulled = true
	return queryPullResult{row: queryRow{}, hasRow: true}
}

func (s *querySingletonStage) close() values.BalValue {
	s.closed = true
	return nil
}

type queryFromStage struct {
	upstream        queryStage
	iteratorFactory queryIteratorFactory
	currentIterator queryIterator
	currentRow      queryRow
	completion      values.BalValue
	done            bool
}

func newQueryFromStage(upstream queryStage, iteratorFactory queryIteratorFactory) *queryFromStage {
	return &queryFromStage{
		upstream:        upstream,
		iteratorFactory: iteratorFactory,
	}
}

func (s *queryFromStage) pull() queryPullResult {
	if s.done {
		return queryPullResult{completion: s.completion}
	}
	for {
		if s.currentIterator != nil {
			value, completion, hasValue := s.currentIterator.pull()
			if hasValue {
				row := make(queryRow, len(s.currentRow), len(s.currentRow)+1)
				copy(row, s.currentRow)
				row = append(row, value)
				return queryPullResult{row: row, hasRow: true}
			}
			s.currentIterator = nil
			if completion != nil {
				return s.finish(completion)
			}
		}

		upstreamResult := s.upstream.pull()
		if !upstreamResult.hasRow {
			return s.finish(upstreamResult.completion)
		}
		s.currentRow = upstreamResult.row
		s.currentIterator = s.iteratorFactory(s.currentRow)
	}
}

func (s *queryFromStage) finish(completion values.BalValue) queryPullResult {
	s.done = true
	s.completion = completion
	return queryPullResult{completion: completion}
}

func (s *queryFromStage) close() values.BalValue {
	if s.done {
		return nil
	}
	s.done = true
	if s.currentIterator != nil {
		if completion := s.currentIterator.close(); completion != nil {
			_ = s.upstream.close()
			return completion
		}
	}
	return s.upstream.close()
}

type queryFilterStage struct {
	upstream   queryStage
	predicate  queryEvaluator
	completion values.BalValue
	done       bool
}

func newQueryFilterStage(upstream queryStage, predicate queryEvaluator) *queryFilterStage {
	return &queryFilterStage{
		upstream:  upstream,
		predicate: predicate,
	}
}

func (s *queryFilterStage) pull() queryPullResult {
	if s.done {
		return queryPullResult{completion: s.completion}
	}
	for {
		result := s.upstream.pull()
		if !result.hasRow {
			s.done = true
			s.completion = result.completion
			return result
		}
		predicateResult := s.predicate(result.row)
		if predicateResult.completion != nil {
			s.done = true
			s.completion = predicateResult.completion
			return queryPullResult{completion: predicateResult.completion}
		}
		if predicateResult.value.(bool) {
			return result
		}
	}
}

func (s *queryFilterStage) close() values.BalValue {
	s.done = true
	return s.upstream.close()
}

type queryMapStage struct {
	upstream   queryStage
	evaluators []queryEvaluator
	completion values.BalValue
	done       bool
}

func newQueryMapStage(upstream queryStage, evaluators []queryEvaluator) *queryMapStage {
	return &queryMapStage{
		upstream:   upstream,
		evaluators: append([]queryEvaluator{}, evaluators...),
	}
}

func (s *queryMapStage) pull() queryPullResult {
	if s.done {
		return queryPullResult{completion: s.completion}
	}
	result := s.upstream.pull()
	if !result.hasRow {
		s.done = true
		s.completion = result.completion
		return result
	}

	row := make(queryRow, len(result.row), len(result.row)+len(s.evaluators))
	copy(row, result.row)
	for _, evaluator := range s.evaluators {
		evalResult := evaluator(row)
		if evalResult.completion != nil {
			s.done = true
			s.completion = evalResult.completion
			return queryPullResult{completion: evalResult.completion}
		}
		row = append(row, evalResult.value)
	}
	return queryPullResult{row: row, hasRow: true}
}

func (s *queryMapStage) close() values.BalValue {
	s.done = true
	return s.upstream.close()
}

type querySelectStage struct {
	upstream   queryStage
	evaluator  queryEvaluator
	completion values.BalValue
	done       bool
}

func newQuerySelectStage(upstream queryStage, evaluator queryEvaluator) *querySelectStage {
	return &querySelectStage{
		upstream:  upstream,
		evaluator: evaluator,
	}
}

func (s *querySelectStage) pull() queryPullResult {
	if s.done {
		return queryPullResult{completion: s.completion}
	}
	result := s.upstream.pull()
	if !result.hasRow {
		s.done = true
		s.completion = result.completion
		return result
	}
	evalResult := s.evaluator(result.row)
	if evalResult.completion != nil {
		s.done = true
		s.completion = evalResult.completion
		return queryPullResult{completion: evalResult.completion}
	}
	return queryPullResult{
		row:    queryRow{evalResult.value},
		hasRow: true,
	}
}

func (s *querySelectStage) close() values.BalValue {
	s.done = true
	return s.upstream.close()
}

type queryLimitStage struct {
	upstream   queryStage
	limit      int64
	count      int64
	completion values.BalValue
	done       bool
}

func newQueryLimitStage(upstream queryStage, limit int64) *queryLimitStage {
	return &queryLimitStage{
		upstream: upstream,
		limit:    max(limit, 0),
	}
}

func (s *queryLimitStage) pull() queryPullResult {
	if s.done {
		return queryPullResult{completion: s.completion}
	}
	if s.count >= s.limit {
		s.done = true
		return queryPullResult{}
	}
	result := s.upstream.pull()
	if !result.hasRow {
		s.done = true
		s.completion = result.completion
		return result
	}
	s.count++
	return result
}

func (s *queryLimitStage) close() values.BalValue {
	s.done = true
	return s.upstream.close()
}
