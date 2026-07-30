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
	"sort"

	"ballerina/semtypes"
	"ballerina/values"
)

type queryRow []values.BalValue

type queryPullResult struct {
	row        queryRow
	completion values.BalValue
	hasRow     bool
	terminal   bool
}

type queryStage interface {
	pull() queryPullResult
	close() values.BalValue
}

type queryIterator interface {
	pull() (values.BalValue, values.BalValue, bool, bool)
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
		return queryPullResult{terminal: true}
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
		return queryPullResult{completion: s.completion, terminal: true}
	}
	for {
		if s.currentIterator != nil {
			value, completion, hasValue, terminal := s.currentIterator.pull()
			if hasValue {
				row := make(queryRow, len(s.currentRow), len(s.currentRow)+1)
				copy(row, s.currentRow)
				row = append(row, value)
				return queryPullResult{row: row, hasRow: true}
			}
			s.currentIterator = nil
			if !terminal {
				return queryPullResult{completion: completion}
			}
			if completion != nil {
				return s.finish(completion)
			}
		}

		upstreamResult := s.upstream.pull()
		if !upstreamResult.hasRow {
			if !upstreamResult.terminal {
				return upstreamResult
			}
			return s.finish(upstreamResult.completion)
		}
		s.currentRow = upstreamResult.row
		s.currentIterator = s.iteratorFactory(s.currentRow)
	}
}

func (s *queryFromStage) finish(completion values.BalValue) queryPullResult {
	s.done = true
	s.completion = completion
	return queryPullResult{completion: completion, terminal: true}
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
		return queryPullResult{completion: s.completion, terminal: true}
	}
	for {
		result := s.upstream.pull()
		if !result.hasRow {
			if !result.terminal {
				return result
			}
			s.done = true
			s.completion = result.completion
			return result
		}
		predicateResult := s.predicate(result.row)
		if predicateResult.completion != nil {
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
		return queryPullResult{completion: s.completion, terminal: true}
	}
	result := s.upstream.pull()
	if !result.hasRow {
		if !result.terminal {
			return result
		}
		s.done = true
		s.completion = result.completion
		return result
	}

	row := make(queryRow, len(result.row), len(result.row)+len(s.evaluators))
	copy(row, result.row)
	for _, evaluator := range s.evaluators {
		evalResult := evaluator(row)
		if evalResult.completion != nil {
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

type queryJoinStage struct {
	upstream            queryStage
	iteratorFactory     queryIteratorFactory
	predicate           queryEvaluator
	outer               bool
	currentIterator     queryIterator
	currentRow          queryRow
	currentMatched      bool
	currentOuterEmitted bool
	rightValues         []values.BalValue
	rightInitialized    bool
	completion          values.BalValue
	done                bool
}

func newQueryJoinStage(
	upstream queryStage,
	iteratorFactory queryIteratorFactory,
	predicate queryEvaluator,
	outer bool,
) *queryJoinStage {
	return &queryJoinStage{
		upstream:        upstream,
		iteratorFactory: iteratorFactory,
		predicate:       predicate,
		outer:           outer,
	}
}

func (s *queryJoinStage) pull() queryPullResult {
	if s.done {
		return queryPullResult{completion: s.completion, terminal: true}
	}
	for {
		if s.currentIterator != nil {
			value, completion, hasValue, terminal := s.currentIterator.pull()
			if hasValue {
				row := make(queryRow, len(s.currentRow), len(s.currentRow)+1)
				copy(row, s.currentRow)
				row = append(row, value)
				predicateResult := s.predicate(row)
				if predicateResult.completion != nil {
					return queryPullResult{completion: predicateResult.completion}
				}
				if predicateResult.value.(bool) {
					s.currentMatched = true
					return queryPullResult{row: row, hasRow: true}
				}
				continue
			}
			s.currentIterator = nil
			if !terminal {
				return queryPullResult{completion: completion}
			}
			if completion != nil {
				return s.finish(completion)
			}
			if s.outer && !s.currentMatched && !s.currentOuterEmitted {
				s.currentOuterEmitted = true
				row := make(queryRow, len(s.currentRow), len(s.currentRow)+1)
				copy(row, s.currentRow)
				row = append(row, nil)
				return queryPullResult{row: row, hasRow: true}
			}
		}

		upstreamResult := s.upstream.pull()
		if !upstreamResult.hasRow {
			if !upstreamResult.terminal {
				return upstreamResult
			}
			return s.finish(upstreamResult.completion)
		}
		s.currentRow = upstreamResult.row
		s.currentMatched = false
		s.currentOuterEmitted = false
		if !s.rightInitialized {
			completion, terminal := s.initializeRight(s.currentRow)
			if completion != nil {
				if terminal {
					return s.finish(completion)
				}
				return queryPullResult{completion: completion}
			}
		}
		s.currentIterator = &queryValuesIterator{values: s.rightValues}
	}
}

func (s *queryJoinStage) initializeRight(row queryRow) (values.BalValue, bool) {
	iterator := s.iteratorFactory(row)
	for {
		value, completion, hasValue, terminal := iterator.pull()
		if !hasValue {
			if terminal {
				s.rightInitialized = completion == nil
			}
			return completion, terminal
		}
		s.rightValues = append(s.rightValues, value)
	}
}

func (s *queryJoinStage) finish(completion values.BalValue) queryPullResult {
	s.done = true
	s.completion = completion
	return queryPullResult{completion: completion, terminal: true}
}

func (s *queryJoinStage) close() values.BalValue {
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

type queryValuesIterator struct {
	values []values.BalValue
	index  int
}

func (i *queryValuesIterator) pull() (values.BalValue, values.BalValue, bool, bool) {
	if i.index >= len(i.values) {
		return nil, nil, false, true
	}
	value := i.values[i.index]
	i.index++
	return value, nil, true, false
}

func (i *queryValuesIterator) close() values.BalValue {
	return nil
}

type queryOrderedRow struct {
	row  queryRow
	keys []values.BalValue
}

type queryOrderStage struct {
	upstream    queryStage
	evaluators  []queryEvaluator
	ascending   []bool
	rows        []queryOrderedRow
	index       int
	completion  values.BalValue
	initialized bool
	done        bool
}

func newQueryOrderStage(
	upstream queryStage,
	evaluators []queryEvaluator,
	ascending []bool,
) *queryOrderStage {
	return &queryOrderStage{
		upstream:   upstream,
		evaluators: append([]queryEvaluator{}, evaluators...),
		ascending:  append([]bool{}, ascending...),
		rows:       make([]queryOrderedRow, 0),
	}
}

func (s *queryOrderStage) pull() queryPullResult {
	if s.done {
		return queryPullResult{completion: s.completion, terminal: true}
	}
	if !s.initialized {
		result, complete := s.materialize()
		if !complete {
			if result.terminal {
				s.done = true
				s.completion = result.completion
			}
			return result
		}
		s.initialized = true
	}
	if s.index >= len(s.rows) {
		s.done = true
		return queryPullResult{terminal: true}
	}
	row := s.rows[s.index].row
	s.index++
	return queryPullResult{row: row, hasRow: true}
}

func (s *queryOrderStage) materialize() (queryPullResult, bool) {
	for {
		result := s.upstream.pull()
		if !result.hasRow {
			if !result.terminal || result.completion != nil {
				return result, false
			}
			break
		}
		keys := make([]values.BalValue, len(s.evaluators))
		for i, evaluator := range s.evaluators {
			evalResult := evaluator(result.row)
			if evalResult.completion != nil {
				return queryPullResult{completion: evalResult.completion}, false
			}
			keys[i] = evalResult.value
		}
		s.rows = append(s.rows, queryOrderedRow{row: result.row, keys: keys})
	}
	sort.SliceStable(s.rows, func(i, j int) bool {
		for keyIndex := range s.evaluators {
			comparison := values.CompareK(
				s.rows[i].keys[keyIndex],
				s.rows[j].keys[keyIndex],
				s.ascending[keyIndex],
			)
			if comparison != values.CmpEQ {
				return comparison == values.CmpLT
			}
		}
		return false
	})
	return queryPullResult{}, true
}

func (s *queryOrderStage) close() values.BalValue {
	s.done = true
	return s.upstream.close()
}

type queryGroup struct {
	keys []values.BalValue
	rows []queryRow
}

type queryGroupStage struct {
	typeCtx           semtypes.Context
	upstream          queryStage
	evaluators        []queryEvaluator
	keyBindingIndices []int64
	scalarBindings    []bool
	outputTypes       []semtypes.SemType
	groups            []queryGroup
	index             int
	completion        values.BalValue
	initialized       bool
	done              bool
}

func newQueryGroupStage(
	typeCtx semtypes.Context,
	upstream queryStage,
	evaluators []queryEvaluator,
	keyBindingIndices []int64,
	scalarBindings []bool,
	outputTypes []semtypes.SemType,
) *queryGroupStage {
	return &queryGroupStage{
		typeCtx:           typeCtx,
		upstream:          upstream,
		evaluators:        append([]queryEvaluator{}, evaluators...),
		keyBindingIndices: append([]int64{}, keyBindingIndices...),
		scalarBindings:    append([]bool{}, scalarBindings...),
		outputTypes:       append([]semtypes.SemType{}, outputTypes...),
		groups:            make([]queryGroup, 0),
	}
}

func (s *queryGroupStage) pull() queryPullResult {
	if s.done {
		return queryPullResult{completion: s.completion, terminal: true}
	}
	if !s.initialized {
		result, complete := s.materialize()
		if !complete {
			if result.terminal {
				s.done = true
				s.completion = result.completion
			}
			return result
		}
		s.initialized = true
	}
	if s.index >= len(s.groups) {
		s.done = true
		return queryPullResult{terminal: true}
	}
	group := &s.groups[s.index]
	s.index++
	return queryPullResult{row: s.groupRow(group), hasRow: true}
}

func (s *queryGroupStage) materialize() (queryPullResult, bool) {
	for {
		result := s.upstream.pull()
		if !result.hasRow {
			if !result.terminal || result.completion != nil {
				return result, false
			}
			return queryPullResult{}, true
		}
		row := append(queryRow{}, result.row...)
		keys := make([]values.BalValue, len(s.evaluators))
		for i, evaluator := range s.evaluators {
			evalResult := evaluator(row)
			if evalResult.completion != nil {
				return queryPullResult{completion: evalResult.completion}, false
			}
			keys[i] = evalResult.value
			if bindingIndex := s.keyBindingIndices[i]; bindingIndex == int64(len(row)) {
				row = append(row, evalResult.value)
			}
		}
		groupIndex := s.findGroup(keys)
		if groupIndex < 0 {
			s.groups = append(s.groups, queryGroup{keys: keys, rows: []queryRow{row}})
			continue
		}
		s.groups[groupIndex].rows = append(s.groups[groupIndex].rows, row)
	}
}

func (s *queryGroupStage) findGroup(keys []values.BalValue) int {
	for i := range s.groups {
		groupKeys := s.groups[i].keys
		if len(groupKeys) != len(keys) {
			continue
		}
		equal := true
		for j := range keys {
			if !values.DeepEquals(groupKeys[j], keys[j]) {
				equal = false
				break
			}
		}
		if equal {
			return i
		}
	}
	return -1
}

func (s *queryGroupStage) groupRow(group *queryGroup) queryRow {
	row := make(queryRow, len(s.scalarBindings))
	for bindingIndex, scalar := range s.scalarBindings {
		if scalar {
			row[bindingIndex] = group.rows[0][bindingIndex]
			continue
		}
		items := make([]values.BalValue, len(group.rows))
		for rowIndex := range group.rows {
			items[rowIndex] = group.rows[rowIndex][bindingIndex]
		}
		outputTy := s.outputTypes[bindingIndex]
		row[bindingIndex] = values.NewList(
			outputTy,
			semtypes.ToListAtomicType(s.typeCtx, outputTy),
			false,
			nil,
			len(items),
			items,
		)
	}
	return row
}

func (s *queryGroupStage) close() values.BalValue {
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
		return queryPullResult{completion: s.completion, terminal: true}
	}
	result := s.upstream.pull()
	if !result.hasRow {
		if result.terminal {
			s.done = true
			s.completion = result.completion
		}
		return result
	}
	evalResult := s.evaluator(result.row)
	if evalResult.completion != nil {
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
	upstream    queryStage
	evaluator   queryEvaluator
	limit       int64
	count       int64
	completion  values.BalValue
	initialized bool
	done        bool
}

func newQueryLimitStage(upstream queryStage, limit int64) *queryLimitStage {
	return newQueryEvaluatedLimitStage(upstream, func(queryRow) queryEvalResult {
		return queryEvalResult{value: limit}
	})
}

func newQueryEvaluatedLimitStage(upstream queryStage, evaluator queryEvaluator) *queryLimitStage {
	return &queryLimitStage{
		upstream:  upstream,
		evaluator: evaluator,
	}
}

func (s *queryLimitStage) pull() queryPullResult {
	if s.done {
		return queryPullResult{completion: s.completion, terminal: true}
	}
	if !s.initialized {
		result := s.upstream.pull()
		if !result.hasRow {
			if result.terminal {
				s.done = true
				s.completion = result.completion
			}
			return result
		}
		evalResult := s.evaluator(result.row)
		if evalResult.completion != nil {
			return queryPullResult{completion: evalResult.completion}
		}
		s.initialized = true
		s.limit = max(evalResult.value.(int64), 0)
		if s.limit == 0 {
			s.done = true
			return queryPullResult{terminal: true}
		}
		s.count = 1
		return result
	}
	if s.count >= s.limit {
		s.done = true
		return queryPullResult{terminal: true}
	}
	result := s.upstream.pull()
	if !result.hasRow {
		if result.terminal {
			s.done = true
			s.completion = result.completion
		}
		return result
	}
	s.count++
	return result
}

func (s *queryLimitStage) close() values.BalValue {
	s.done = true
	return s.upstream.close()
}
