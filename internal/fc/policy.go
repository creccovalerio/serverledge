package fc

type FcPolicy interface {
	Init()
	OnCompletion(request *scheduledFcRequest)
	OnArrival(request *scheduledFcRequest)
}
