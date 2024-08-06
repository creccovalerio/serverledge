package executor

// InvocationRequest is a struct used by the executor to effectively run the function
type InvocationRequest struct {
	Command         []string
	Params          map[string]interface{}
	Handler         string
	HandlerDir      string
	IsInComposition bool
	ReturnOutput bool
}

type InvocationResult struct {
	Success bool
	Result  string
	Output  string // TODO what's the difference with Result?
}
