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
	"fmt"

	"ballerina/bir"
	"ballerina/runtime/extern"
	"ballerina/semtypes"
	"ballerina/values"
)

func execNewQueryStream(ctx *extern.Context, instr *bir.NewQueryStream, frame *Frame) {
	var stage queryStage = &querySingletonStage{}
	for _, clause := range instr.Clauses {
		evaluators := make([]queryEvaluator, len(clause.EvaluatorOps))
		for i, evaluatorOp := range clause.EvaluatorOps {
			evaluators[i] = queryEvaluatorFromOperand(ctx, evaluatorOp, frame)
		}
		switch clause.Kind {
		case bir.QueryClauseFrom:
			stage = newQueryFromStage(stage, queryIteratorFactoryFromEvaluator(ctx, evaluators[0]))
		case bir.QueryClauseWhere:
			stage = newQueryFilterStage(stage, evaluators[0])
		case bir.QueryClauseLet:
			stage = newQueryMapStage(stage, evaluators)
		case bir.QueryClauseJoin:
			stage = newQueryJoinStage(
				stage,
				queryIteratorFactoryFromEvaluator(ctx, evaluators[0]),
				evaluators[1],
				evaluators[2],
				clause.BoolArgs[0],
			)
		case bir.QueryClauseOrderBy:
			stage = newQueryOrderStage(stage, evaluators, clause.BoolArgs)
		case bir.QueryClauseGroupBy:
			stage = newQueryGroupStage(
				ctx.TypeCtx,
				stage,
				evaluators,
				clause.IntArgs,
				clause.BoolArgs,
				clause.TypeArgs,
			)
		case bir.QueryClauseLimit:
			stage = newQueryEvaluatedLimitStage(stage, evaluators[0])
		case bir.QueryClauseSelect:
			stage = newQuerySelectStage(stage, evaluators[0])
		default:
			panic(fmt.Sprintf("unsupported query clause kind: %d", clause.Kind))
		}
	}

	valueTy := semtypes.StreamValueType(ctx.TypeCtx, instr.StreamType)
	recordDef := semtypes.NewMappingDefinition()
	recordTy := recordDef.DefineMappingTypeWrapped(
		ctx.Env.TypeEnv,
		[]semtypes.Field{semtypes.FieldFrom("value", valueTy, false, false)},
		semtypes.NEVER,
	)
	recordAtomicTy := semtypes.ToMappingAtomicType(ctx.TypeCtx, recordTy)
	pipeline := newQueryPipeline(stage)
	next := func() values.BalValue {
		result := pipeline.pull()
		if result.hasRow {
			return values.NewMap(recordTy, recordAtomicTy, false, []values.MapEntry{{
				Key:   "value",
				Value: result.row[0],
			}})
		}
		return result.completion
	}
	stream := values.NewStream(instr.StreamType, next, pipeline.close)
	setOperandValue(ctx, instr.LhsOp, frame, stream)
}

func queryEvaluatorFromOperand(ctx *extern.Context, op *bir.BIROperand, frame *Frame) queryEvaluator {
	fnValue := getOperandValue(ctx, op, frame).(*values.Function)
	handle, err := NewFunctionValueHandle(ctx.Env, fnValue)
	if err != nil {
		panic(err)
	}
	return func(row queryRow) queryEvalResult {
		result, err := handle.invoke(ctx, []values.BalValue(row))
		if err != nil {
			panic(err)
		}
		if wrapped, ok := result.(*values.List); ok && wrapped.Len() == 1 {
			return queryEvalResult{value: wrapped.Get(0)}
		}
		return queryEvalResult{completion: result}
	}
}

func queryIteratorFactoryFromEvaluator(ctx *extern.Context, evaluator queryEvaluator) queryIteratorFactory {
	return func(row queryRow) queryIterator {
		result := evaluator(row)
		if result.completion != nil {
			return &queryCompletionIterator{completion: result.completion}
		}
		return queryIteratorForValue(ctx, result.value)
	}
}

type queryCompletionIterator struct {
	completion values.BalValue
}

func (i *queryCompletionIterator) pull() (values.BalValue, values.BalValue, bool, bool) {
	return nil, i.completion, false, false
}

func (i *queryCompletionIterator) close() values.BalValue {
	return nil
}

type queryListIterator struct {
	list  *values.List
	index int
}

func (i *queryListIterator) pull() (values.BalValue, values.BalValue, bool, bool) {
	if i.index >= i.list.Len() {
		return nil, nil, false, true
	}
	value := i.list.Get(i.index)
	i.index++
	return value, nil, true, false
}

func (i *queryListIterator) close() values.BalValue {
	return nil
}

type queryMapIterator struct {
	mapping *values.Map
	keys    []string
	index   int
}

func (i *queryMapIterator) pull() (values.BalValue, values.BalValue, bool, bool) {
	if i.index >= len(i.keys) {
		return nil, nil, false, true
	}
	value, _ := i.mapping.Get(i.keys[i.index])
	i.index++
	return value, nil, true, false
}

func (i *queryMapIterator) close() values.BalValue {
	return nil
}

type queryStringIterator struct {
	chars []rune
	index int
}

func (i *queryStringIterator) pull() (values.BalValue, values.BalValue, bool, bool) {
	if i.index >= len(i.chars) {
		return nil, nil, false, true
	}
	value := string(i.chars[i.index])
	i.index++
	return value, nil, true, false
}

func (i *queryStringIterator) close() values.BalValue {
	return nil
}

type queryXMLIterator struct {
	items []values.XMLValue
	index int
}

func (i *queryXMLIterator) pull() (values.BalValue, values.BalValue, bool, bool) {
	if i.index >= len(i.items) {
		return nil, nil, false, true
	}
	value := i.items[i.index]
	i.index++
	return value, nil, true, false
}

func (i *queryXMLIterator) close() values.BalValue {
	return nil
}

type queryStreamIterator struct {
	stream *values.Stream
}

func (i *queryStreamIterator) pull() (values.BalValue, values.BalValue, bool, bool) {
	return queryIteratorNextResult(i.stream.Next())
}

func (i *queryStreamIterator) close() values.BalValue {
	return i.stream.Close()
}

type queryObjectIterator struct {
	ctx        *extern.Context
	iterator   *values.Object
	nextHandle extern.MethodHandle
}

func (i *queryObjectIterator) pull() (values.BalValue, values.BalValue, bool, bool) {
	result, err := i.ctx.InvokeMethod(i.nextHandle, []values.BalValue{i.iterator})
	if err != nil {
		panic(err)
	}
	return queryIteratorNextResult(result)
}

func (i *queryObjectIterator) close() values.BalValue {
	return nil
}

func queryIteratorForValue(ctx *extern.Context, value values.BalValue) queryIterator {
	switch value := value.(type) {
	case *values.List:
		return &queryListIterator{list: value}
	case *values.Map:
		return &queryMapIterator{mapping: value, keys: value.Keys()}
	case string:
		return &queryStringIterator{chars: []rune(value)}
	case values.XMLValue:
		return &queryXMLIterator{items: value.IterItems()}
	case *values.Stream:
		return &queryStreamIterator{stream: value}
	case *values.Object:
		return queryIteratorForObject(ctx, value)
	default:
		panic(fmt.Sprintf("unsupported query collection value: %T", value))
	}
}

func queryIteratorForObject(ctx *extern.Context, iterable *values.Object) queryIterator {
	iteratorHandle, ok := ctx.LookupObjectMethod(iterable, "iterator")
	if !ok {
		panic(values.NewErrorWithMessage("query iterable missing 'iterator' method"))
	}
	iteratorValue, err := ctx.InvokeMethod(iteratorHandle, []values.BalValue{iterable})
	if err != nil {
		panic(err)
	}
	iterator := iteratorValue.(*values.Object)
	nextHandle, ok := ctx.LookupObjectMethod(iterator, "next")
	if !ok {
		panic(values.NewErrorWithMessage("query iterator missing 'next' method"))
	}
	result := &queryObjectIterator{
		ctx:        ctx,
		iterator:   iterator,
		nextHandle: nextHandle,
	}
	return result
}

func queryIteratorNextResult(result values.BalValue) (values.BalValue, values.BalValue, bool, bool) {
	if result == nil {
		return nil, nil, false, true
	}
	if _, isError := result.(*values.Error); isError {
		return nil, result, false, true
	}
	record, ok := result.(*values.Map)
	if !ok {
		panic(fmt.Sprintf("query iterator next returned %T instead of a value record", result))
	}
	value, ok := record.Get("value")
	if !ok {
		panic(values.NewErrorWithMessage("query iterator next result missing 'value' field"))
	}
	return value, nil, true, false
}
