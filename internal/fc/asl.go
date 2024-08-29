package fc

import (
	"fmt"
	"github.com/grussorusso/serverledge/internal/asl"
	"github.com/grussorusso/serverledge/internal/function"
)

// FromASL parses a AWS State Language specification file and returns a Function Composition with the corresponding Serverledge Dag
// The name of the composition should not be the file name by default, to avoid problems when adding the same composition multiple times.
func FromASL(name string, aslSrc []byte) (*FunctionComposition, error) {
	stateMachine, err := asl.ParseFrom(name, aslSrc)
	if err != nil {
		return nil, fmt.Errorf("could not parse the ASL file: %v", err)
	}
	dag, err := FromStateMachine(stateMachine)
	if err != nil {
		return nil, fmt.Errorf("failed to convert ASL State Machine to Serverledge DAG: %v", err)
	}

	// we do not care whether function names are duplicate, we handle this in the composition
	funcNames := stateMachine.GetFunctionNames()
	functions := make([]*function.Function, 0)
	for _, f := range funcNames {
		funcObj, ok := function.GetFunction(f)
		if !ok {
			return nil, fmt.Errorf("function does not exists")
		}
		functions = append(functions, funcObj)
	}

	comp := NewFC(stateMachine.Name, *dag, functions, true)
	return &comp, nil
}

/* ============== Build from ASL States =================== */

// BuildFromTaskState adds a SimpleNode to the previous Node. The simple node will have id as specified by the name parameter
func BuildFromTaskState(builder *DagBuilder, t *asl.TaskState, name string) (*DagBuilder, error) {
	f, found := function.GetFunction(t.Resource) // Could have been used t.GetResources()[0], but it is better to avoid the array dereference
	if !found {
		return nil, fmt.Errorf("non existing function in composition: %s", t.Resource)
	}
	builder = builder.AddSimpleNodeWithId(f, name)
	fmt.Printf("Added simple node with f: %s\n", f.Name)
	return builder, nil
}

// BuildFromChoiceState adds a ChoiceNode as defined in the ChoiceState, connects it to the previous Node, and TERMINATES the DAG
func BuildFromChoiceState(builder *DagBuilder, c *asl.ChoiceState, name string, entireSM *asl.StateMachine) (*Dag, error) {
	conds, err := BuildConditionFromRule(c.Choices)
	if err != nil {
		return nil, err
	}
	branchBuilder := builder.AddChoiceNode(conds...)

	// the choice state has two or more StateMachine(s) in it, one for each branch
	i := 0
	for branchBuilder.HasNextBranch() {
		nextState := c.Choices[i].GetNextState()
		sm, errBranch := GetBranchesForChoiceFromStates(entireSM, nextState, i)
		if errBranch != nil {
			return nil, errBranch
		}
		branchBuilder = branchBuilder.NextBranch(FromStateMachine(sm))
		i++
	}
	// TODO: handle the default branch

	return builder.Build()
}

// BuildConditionFromRule creates a condition from a rule
func BuildConditionFromRule(rules []asl.ChoiceRule) ([]Condition, error) {
	conds := make([]Condition, 0)

	for _, rule := range rules {
		switch t := rule.(type) {
		case *asl.BooleanExpression:
			break
		case *asl.DataTestExpression:
			param := NewParam(t.Test.Variable)
			val := NewValue(t.Test.ComparisonOperator.Operand)
			// TODO: handle the case when there are two params!!!
			var condition Condition
			switch t.Test.ComparisonOperator.Kind {
			case "StringEquals":
				condition = NewEqParamCondition(param, val)
				break
			case "StringEqualsPath":
			case "StringLessThan":
			case "StringLessThanPath":
			case "StringGreaterThan":
			case "StringGreaterThanPath":
			case "StringLessThanEquals":
			case "StringLessThanEqualsPath":
			case "StringGreaterThanEquals":
			case "StringGreaterThanEqualsPath":
			case "StringMatches":
				panic("Not implemented")
			case "NumericEquals":
				condition = NewEqParamCondition(param, val)
				break
			case "NumericEqualsPath":
				panic("Not implemented")
			case "NumericLessThan":
				condition = NewSmallerParamCondition(param, val)
				break
			case "NumericLessThanPath":
				panic("Not implemented")
			case "NumericGreaterThan":
				condition = NewGreaterParamCondition(param, val)
				break
			case "NumericGreaterThanPath":
				panic("Not implemented")
			case "NumericLessThanEquals":
				condition = NewOr(NewSmallerParamCondition(param, val), NewEqParamCondition(param, val))
				break
			case "NumericLessThanEqualsPath":
				panic("Not implemented")
			case "NumericGreaterThanEquals":
				condition = NewOr(NewGreaterParamCondition(param, val), NewEqParamCondition(param, val))
				break
			case "NumericGreaterThanEqualsPath":
				panic("Not implemented")
			case "BooleanEquals":
				condition = NewEqCondition(param, true)
				break
			case "BooleanEqualsPath":
				panic("Not implemented")
			case "TimestampEquals":
			case "TimestampEqualsPath":
			case "TimestampLessThan":
			case "TimestampLessThanPath":
			case "TimestampGreaterThan":
			case "TimestampGreaterThanPath":
			case "TimestampLessThanEquals":
			case "TimestampLessThanEqualsPath":
			case "TimestampGreaterThanEquals":
			case "TimestampGreaterThanEqualsPath":
				panic("Not implemented")
			case "IsNull":
				condition = NewEqCondition(param, nil)
				break
			case "IsPresent":
				condition = NewNot(NewEqCondition(param, nil))
				break
			case "IsNumeric":
				break
			case "IsString":
				break
			case "IsBoolean":
				condition = NewOr(NewEqCondition(param, true), NewEqCondition(param, false))
			case "IsTimestamp":
				panic("Not implemented")
			}
			conds = append(conds, condition)
			break
		default:
			return []Condition{}, fmt.Errorf("this is not a ChoiceRule: %v", rule)
		}
	}

	// this is for the default branch
	conds = append(conds, NewConstCondition(true))
	return conds, nil
}

func GetBranchesForChoiceFromStates(sm *asl.StateMachine, nextState string, branchIndex int) (*asl.StateMachine, error) {
	fmt.Printf("Branch index: %d\n", branchIndex)

	return sm, nil
}

// BuildFromParallelState adds a FanOutNode and a FanInNode and as many branches as defined in the ParallelState
func BuildFromParallelState(builder *DagBuilder, c *asl.ParallelState, name string) (*DagBuilder, error) {
	// TODO: implement me
	return builder, nil
}

// BuildFromMapState is not compatible with Serverledge at the moment
func BuildFromMapState(builder *DagBuilder, c *asl.MapState, name string) (*DagBuilder, error) {
	// TODO: implement me
	// TODO: implement MapNode
	panic("not compatible with serverledge currently")
	// return builder, nil
}

// BuildFromPassState adds a SimpleNode with an identity function
func BuildFromPassState(builder *DagBuilder, p *asl.PassState, name string) (*DagBuilder, error) {
	// TODO: implement me
	return builder, nil
}

// BuildFromWaitState adds a Simple node with a sleep function for the specified time as described in the WaitState
func BuildFromWaitState(builder *DagBuilder, w *asl.WaitState, name string) (*DagBuilder, error) {
	// TODO: implement me
	return builder, nil
}

// BuildFromSucceedState is not fully compatible with serverledge, but it adds an EndNode
func BuildFromSucceedState(builder *DagBuilder, s *asl.SucceedState, name string) (*DagBuilder, error) {
	// TODO: implement me
	return builder, nil
}

// BuildFromFailState is not fully compatible with serverledge, but it adds an EndNode
func BuildFromFailState(builder *DagBuilder, s *asl.FailState, name string) (*DagBuilder, error) {
	// TODO: implement me
	return builder, nil
}
