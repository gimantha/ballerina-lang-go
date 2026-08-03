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

package desugar

import (
	"ballerina/ast"
	"ballerina/model"
	"ballerina/semtypes"
)

type LazyQueryClauseKind uint8

const (
	LazyQueryClauseFrom LazyQueryClauseKind = iota
	LazyQueryClauseWhere
	LazyQueryClauseLet
	LazyQueryClauseJoin
	LazyQueryClauseOrderBy
	LazyQueryClauseGroupBy
	LazyQueryClauseLimit
	LazyQueryClauseSelect
)

type LazyQueryClause struct {
	Kind       LazyQueryClauseKind
	Evaluators []*ast.BLangLambdaFunction
	BoolArgs   []bool
	IntArgs    []int64
	TypeArgs   []semtypes.SemType
}

// BLangLazyQueryExpr is emitted after semantic analysis and consumed directly
// by BIR generation. Evaluator results use a one-element list as the success
// tag; an unwrapped value is an abrupt pipeline completion.
type BLangLazyQueryExpr struct {
	ast.AbstractExpression
	Clauses []LazyQueryClause
}

var (
	_ ast.BLangExpression      = &BLangLazyQueryExpr{}
	_ ast.BLangNode            = &BLangLazyQueryExpr{}
	_ ast.ExternalWalkableNode = &BLangLazyQueryExpr{}
)

func (e *BLangLazyQueryExpr) WalkChildren(visitor ast.Visitor) {
	for _, clause := range e.Clauses {
		for _, evaluator := range clause.Evaluators {
			ast.Walk(visitor, evaluator)
		}
	}
}

func walkLazyQueryExpr(
	cx *functionContext,
	expr *ast.BLangQueryExpr,
) desugaredNode[ast.BLangActionOrExpression] {
	lazyExpr := &BLangLazyQueryExpr{}
	lazyExpr.SetDeterminedType(expr.GetDeterminedType())
	lazyExpr.SetPosition(expr.GetPosition())

	evaluatorIndex := 0
	createEvaluator := func(
		expr ast.BLangActionOrExpression,
		bindings []queryRowBinding,
	) (*ast.BLangLambdaFunction, bool) {
		fnName := lazyQueryFunctionName(lazyExpr.GetPosition(), evaluatorIndex)
		evaluatorIndex++
		return createLazyQueryEvaluator(cx, expr, bindings, fnName)
	}

	bindings := make([]queryRowBinding, 0)
	for _, queryClause := range expr.QueryClauseList {
		switch clause := queryClause.(type) {
		case *ast.BLangFromClause:
			evaluator, ok := createEvaluator(clause.Collection, bindings)
			if !ok {
				return desugaredNode[ast.BLangActionOrExpression]{replacementNode: expr}
			}
			binding, ok := queryRowBindingFromVarDef(cx, clause.VariableDefinitionNode, "from")
			if !ok {
				return desugaredNode[ast.BLangActionOrExpression]{replacementNode: expr}
			}
			lazyExpr.Clauses = append(lazyExpr.Clauses, LazyQueryClause{
				Kind:       LazyQueryClauseFrom,
				Evaluators: []*ast.BLangLambdaFunction{evaluator},
			})
			bindings = append(bindings, binding)
		case *ast.BLangJoinClause:
			collectionEvaluator, ok := createEvaluator(clause.Collection, nil)
			if !ok {
				return desugaredNode[ast.BLangActionOrExpression]{replacementNode: expr}
			}
			binding, ok := queryRowBindingFromVarDef(cx, clause.VariableDefinitionNode, "join")
			if !ok {
				return desugaredNode[ast.BLangActionOrExpression]{replacementNode: expr}
			}
			leftKey, leftOK := clause.OnClause.OnExpr.(ast.BLangExpression)
			rightKey, rightOK := clause.OnClause.EqualsExpr.(ast.BLangExpression)
			if !leftOK || !rightOK {
				cx.internalError("lazy query join keys must be expressions")
				return desugaredNode[ast.BLangActionOrExpression]{replacementNode: expr}
			}
			leftKeyEvaluator, ok := createEvaluator(leftKey, bindings)
			if !ok {
				return desugaredNode[ast.BLangActionOrExpression]{replacementNode: expr}
			}
			rightKeyEvaluator, ok := createEvaluator(rightKey, []queryRowBinding{binding})
			if !ok {
				return desugaredNode[ast.BLangActionOrExpression]{replacementNode: expr}
			}
			lazyExpr.Clauses = append(lazyExpr.Clauses, LazyQueryClause{
				Kind: LazyQueryClauseJoin,
				Evaluators: []*ast.BLangLambdaFunction{
					collectionEvaluator,
					leftKeyEvaluator,
					rightKeyEvaluator,
				},
				BoolArgs: []bool{clause.IsOuterJoinFlag},
			})
			bindings = append(bindings, binding)
		case *ast.BLangWhereClause:
			evaluator, ok := createEvaluator(clause.Expression, bindings)
			if !ok {
				return desugaredNode[ast.BLangActionOrExpression]{replacementNode: expr}
			}
			lazyExpr.Clauses = append(lazyExpr.Clauses, LazyQueryClause{
				Kind:       LazyQueryClauseWhere,
				Evaluators: []*ast.BLangLambdaFunction{evaluator},
			})
		case *ast.BLangLetClause:
			evaluators := make([]*ast.BLangLambdaFunction, 0, len(clause.LetVarDeclarations))
			for i := range clause.LetVarDeclarations {
				varDef := &clause.LetVarDeclarations[i]
				binding, ok := queryRowBindingFromVarDef(cx, varDef, "let")
				if !ok || varDef.Var.Expr == nil {
					return desugaredNode[ast.BLangActionOrExpression]{replacementNode: expr}
				}
				evaluator, ok := createEvaluator(varDef.Var.Expr, bindings)
				if !ok {
					return desugaredNode[ast.BLangActionOrExpression]{replacementNode: expr}
				}
				evaluators = append(evaluators, evaluator)
				bindings = append(bindings, binding)
			}
			lazyExpr.Clauses = append(lazyExpr.Clauses, LazyQueryClause{
				Kind:       LazyQueryClauseLet,
				Evaluators: evaluators,
			})
		case *ast.BLangOrderByClause:
			evaluators := make([]*ast.BLangLambdaFunction, len(clause.OrderByKeyList))
			ascending := make([]bool, len(clause.OrderByKeyList))
			for i := range clause.OrderByKeyList {
				orderKey := &clause.OrderByKeyList[i]
				evaluator, ok := createEvaluator(orderKey.Expression, bindings)
				if !ok {
					return desugaredNode[ast.BLangActionOrExpression]{replacementNode: expr}
				}
				evaluators[i] = evaluator
				ascending[i] = !orderKey.IsDescending
			}
			lazyExpr.Clauses = append(lazyExpr.Clauses, LazyQueryClause{
				Kind:       LazyQueryClauseOrderBy,
				Evaluators: evaluators,
				BoolArgs:   ascending,
			})
		case *ast.BLangGroupByClause:
			groupingSymbols := make(map[model.SymbolRef]bool)
			groupBindings := append([]queryRowBinding{}, bindings...)
			evaluators := make([]*ast.BLangLambdaFunction, 0, len(clause.GroupingKeyList))
			keyBindingIndices := make([]int64, 0, len(clause.GroupingKeyList))
			for i := range clause.GroupingKeyList {
				groupingKey := &clause.GroupingKeyList[i]
				switch {
				case groupingKey.VariableRef != nil:
					symbol := cx.pkgCtx.compilerCtx.UnnarrowedSymbol(groupingKey.VariableRef.Symbol())
					bindingIndex := lazyQueryBindingIndex(cx, groupBindings, symbol)
					if bindingIndex < 0 {
						cx.internalError("lazy query grouping variable is not bound")
						return desugaredNode[ast.BLangActionOrExpression]{replacementNode: expr}
					}
					evaluator, ok := createEvaluator(groupingKey.VariableRef, groupBindings)
					if !ok {
						return desugaredNode[ast.BLangActionOrExpression]{replacementNode: expr}
					}
					groupingSymbols[symbol] = true
					groupingSymbols[groupBindings[bindingIndex].symbol] = true
					evaluators = append(evaluators, evaluator)
					keyBindingIndices = append(keyBindingIndices, int64(bindingIndex))
				case groupingKey.VariableDef != nil:
					varDef := groupingKey.VariableDef
					if varDef.Var == nil || varDef.Var.Expr == nil {
						cx.internalError("lazy query grouping variable must have an initializer")
						return desugaredNode[ast.BLangActionOrExpression]{replacementNode: expr}
					}
					evaluator, ok := createEvaluator(varDef.Var.Expr, groupBindings)
					if !ok {
						return desugaredNode[ast.BLangActionOrExpression]{replacementNode: expr}
					}
					bindingIndex := int64(-1)
					if queryVarDefHasBindableSymbol(varDef) {
						binding, ok := queryRowBindingFromVarDef(cx, varDef, "group by")
						if !ok {
							return desugaredNode[ast.BLangActionOrExpression]{replacementNode: expr}
						}
						bindingIndex = int64(len(groupBindings))
						groupingSymbols[cx.pkgCtx.compilerCtx.UnnarrowedSymbol(binding.symbol)] = true
						groupingSymbols[binding.symbol] = true
						groupBindings = append(groupBindings, binding)
					}
					evaluators = append(evaluators, evaluator)
					keyBindingIndices = append(keyBindingIndices, bindingIndex)
				default:
					cx.internalError("lazy query grouping key shape is invalid")
					return desugaredNode[ast.BLangActionOrExpression]{replacementNode: expr}
				}
			}
			scalarBindings := make([]bool, len(groupBindings))
			for i, binding := range groupBindings {
				symbol := cx.pkgCtx.compilerCtx.UnnarrowedSymbol(binding.symbol)
				scalarBindings[i] = groupingSymbols[symbol]
			}
			outputBindings := queryGroupOutputBindings(cx, groupBindings, groupingSymbols)
			outputTypes := make([]semtypes.SemType, len(outputBindings))
			for i, binding := range outputBindings {
				outputTypes[i] = binding.valueTy
			}
			lazyExpr.Clauses = append(lazyExpr.Clauses, LazyQueryClause{
				Kind:       LazyQueryClauseGroupBy,
				Evaluators: evaluators,
				BoolArgs:   scalarBindings,
				IntArgs:    keyBindingIndices,
				TypeArgs:   outputTypes,
			})
			bindings = outputBindings
		case *ast.BLangLimitClause:
			evaluator, ok := createEvaluator(clause.Expression, nil)
			if !ok {
				return desugaredNode[ast.BLangActionOrExpression]{replacementNode: expr}
			}
			lazyExpr.Clauses = append(lazyExpr.Clauses, LazyQueryClause{
				Kind:       LazyQueryClauseLimit,
				Evaluators: []*ast.BLangLambdaFunction{evaluator},
			})
		case *ast.BLangSelectClause:
			evaluator, ok := createEvaluator(clause.Expression, bindings)
			if !ok {
				return desugaredNode[ast.BLangActionOrExpression]{replacementNode: expr}
			}
			lazyExpr.Clauses = append(lazyExpr.Clauses, LazyQueryClause{
				Kind:       LazyQueryClauseSelect,
				Evaluators: []*ast.BLangLambdaFunction{evaluator},
			})
		default:
			cx.unimplemented("lazy stream query clause is not supported yet")
			return desugaredNode[ast.BLangActionOrExpression]{replacementNode: expr}
		}
	}

	return desugaredNode[ast.BLangActionOrExpression]{replacementNode: lazyExpr}
}

func lazyQueryBindingIndex(
	cx *functionContext,
	bindings []queryRowBinding,
	symbol model.SymbolRef,
) int {
	for i, binding := range bindings {
		if cx.pkgCtx.compilerCtx.UnnarrowedSymbol(binding.symbol) == symbol {
			return i
		}
	}
	return -1
}

func createLazyQueryEvaluator(
	cx *functionContext,
	expr ast.BLangActionOrExpression,
	bindings []queryRowBinding,
	fnName string,
) (*ast.BLangLambdaFunction, bool) {
	pos := expr.GetPosition()
	valueTy := expr.GetDeterminedType()
	if semtypes.IsZero(valueTy) {
		cx.internalError("lazy query evaluator expression type is not resolved")
		return nil, false
	}
	valueExpr, ok := expr.(ast.BLangExpression)
	if !ok {
		cx.unimplemented("actions in lazy query clause callbacks are not supported yet")
		return nil, false
	}

	wrapperDef := semtypes.NewListDefinition()
	wrapperTy := wrapperDef.TupleTypeWrapped(cx.typeEnv(), valueTy)
	wrapper := &ast.BLangListConstructorExpr{
		Exprs: []ast.BLangExpression{valueExpr},
	}
	wrapper.SetDeterminedType(wrapperTy)
	wrapper.AtomicType = *semtypes.ToListAtomicType(cx.typeCtx(), wrapperTy)
	setPositionIfMissing(wrapper, pos)

	paramTypes := make([]semtypes.SemType, len(bindings))
	paramNames := make([]string, len(bindings))
	for i, binding := range bindings {
		paramTypes[i] = binding.valueTy
		paramNames[i] = binding.varName.GetValue()
	}
	returnTy := semtypes.Union(wrapperTy, semtypes.ERROR)
	paramListDef := semtypes.NewListDefinition()
	paramListTy := paramListDef.DefineListTypeWrapped(
		cx.typeEnv(),
		paramTypes,
		len(paramTypes),
		semtypes.NEVER,
		semtypes.CellMutability_CELL_MUT_NONE,
	)
	fnDef := semtypes.NewFunctionDefinition()
	fnTy := fnDef.Define(
		cx.typeEnv(),
		paramListTy,
		returnTy,
		semtypes.FunctionQualifiersFrom(cx.typeEnv(), false, false),
	)

	fnSymbol := model.NewFunctionSymbol(fnName, model.FunctionSignature{
		ParamTypes: paramTypes,
		ParamNames: paramNames,
		ReturnType: returnTy,
	}, false, pos)
	cx.currentScope().AddSymbol(fnName, fnSymbol)
	fnSymbolRef, _ := cx.currentScope().GetSymbol(fnName)
	cx.setSymbolType(fnSymbolRef, fnTy)

	fnScope := cx.newFunctionScope(cx.currentScope())
	symbolMapping := make(map[model.SymbolRef]model.SymbolRef, len(bindings))
	fn := &ast.BLangFunction{}
	fn.Name = &ast.BLangIdentifier{Value: fnName}
	fn.Name.SetDeterminedType(semtypes.NEVER)
	fn.Name.SetPosition(pos)
	fn.SetSymbol(fnSymbolRef)
	fn.SetScope(fnScope)
	fn.SetDeterminedType(semtypes.NEVER)
	fn.SetPosition(pos)
	for _, binding := range bindings {
		paramName := cx.nextDesugarSymbolName()
		paramSymbol := model.NewVariableSymbol(paramName, false, false, true, pos)
		fnScope.AddSymbol(paramName, &paramSymbol)
		paramSymbolRef, _ := fnScope.GetSymbol(paramName)
		cx.setSymbolType(paramSymbolRef, binding.valueTy)

		param := &ast.BLangSimpleVariable{
			Name: &ast.BLangIdentifier{Value: paramName},
		}
		param.Name.SetDeterminedType(semtypes.NEVER)
		param.Name.SetPosition(pos)
		param.SetRequiredParam()
		param.SetSymbol(paramSymbolRef)
		param.SetDeterminedType(binding.valueTy)
		param.SetPosition(pos)
		fn.AddParameter(param)
		symbolMapping[cx.pkgCtx.compilerCtx.UnnarrowedSymbol(binding.symbol)] = paramSymbolRef
	}
	remapLazyQueryBindingRefs(cx, wrapper, symbolMapping)

	returnStmt := &ast.BLangReturn{Expr: wrapper}
	returnStmt.SetDeterminedType(semtypes.NEVER)
	returnStmt.SetPosition(pos)
	body := &ast.BLangBlockFunctionBody{
		Stmts: []ast.StatementNode{returnStmt},
	}
	body.SetDeterminedType(semtypes.NEVER)
	body.SetPosition(pos)
	fn.Body = body

	lambda := &ast.BLangLambdaFunction{Function: desugarFunction(cx.pkgCtx, fn)}
	lambda.SetDeterminedType(fnTy)
	lambda.SetPosition(pos)
	return lambda, true
}

type lazyQueryBindingRemapper struct {
	cx      *functionContext
	mapping map[model.SymbolRef]model.SymbolRef
}

func (r lazyQueryBindingRemapper) Visit(node ast.BLangNode) ast.Visitor {
	if ref, ok := node.(ast.BNodeWithSymbol); ok && ast.SymbolIsSet(ref) {
		symbol := r.cx.pkgCtx.compilerCtx.UnnarrowedSymbol(ref.Symbol())
		if replacement, found := r.mapping[symbol]; found {
			ref.SetSymbol(replacement)
		}
	}
	return r
}

func (r lazyQueryBindingRemapper) VisitTypeData(*ast.TypeData) ast.Visitor {
	return r
}

func remapLazyQueryBindingRefs(
	cx *functionContext,
	node ast.BLangNode,
	mapping map[model.SymbolRef]model.SymbolRef,
) {
	if len(mapping) == 0 {
		return
	}
	ast.Walk(lazyQueryBindingRemapper{cx: cx, mapping: mapping}, node)
}
