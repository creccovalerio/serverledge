package fc

import (
	"fmt"
	"math"
)

type ReqId string

// local cache TODO: usare una vera cache!!!
var progressCache = newProgressCache()

// TODO: add progress to FunctionComposition CompositionRequest (maybe doesn't exists)
// Progress tracks the progress of a Dag, i.e. which nodes are executed, and what is the next node to run. Dag progress is saved in ETCD and retrieved by the next node
type Progress struct {
	ReqId     ReqId // requestId, used to distinguish different dag's progresses
	DagNodes  []*DagNodeInfo
	NextGroup int
}

type ProgressCache struct {
	progresses map[ReqId]*Progress
}

func newProgressCache() ProgressCache {
	return ProgressCache{
		progresses: make(map[ReqId]*Progress),
	}
}

type DagNodeInfo struct {
	Id     DagNodeId
	Type   DagNodeType
	Status DagNodeStatus
	Group  int // The group helps represent the order of execution of nodes. Nodes with the same group should run concurrently
	Branch int // copied from dagNode
}

func newNodeInfo(dNode DagNode, group int) *DagNodeInfo {
	return &DagNodeInfo{
		Id:     dNode.GetId(),
		Type:   parseType(dNode),
		Status: Pending,
		Group:  group,
		Branch: dNode.GetBranchId(),
	}
}

func (ni *DagNodeInfo) Equals(ni2 *DagNodeInfo) bool {
	return ni.Id == ni2.Id && ni.Type == ni2.Type && ni.Status == ni2.Status && ni.Group == ni2.Group && ni.Branch == ni2.Branch
}

type DagNodeStatus int

const (
	Pending = iota
	Executed
	Skipped // if a node is skipped, all its children nodes should also be skipped
	Failed
	Waiting // only fan in node should wait (in a goroutine!!). When invoking a fanIn, if it is already waiting, we do not to nothing and free the serverledge node execution
)

func printStatus(s DagNodeStatus) string {
	switch s {
	case Pending:
		return "Pending"
	case Executed:
		return "Executed"
	case Skipped:
		return "Skipped"
	case Failed:
		return "Failed"
	}
	return "No Status - Error"
}

type DagNodeType int

const (
	Start = iota
	End
	Simple
	Choice
	FanOut
	FanIn
)

func parseType(dNode DagNode) DagNodeType {
	switch dNode.(type) {
	case *StartNode:
		return Start
	case *EndNode:
		return End
	case *SimpleNode:
		return Simple
	case *ChoiceNode:
		return Choice
	case *FanOutNode:
		return FanOut
	case *FanInNode:
		return FanIn
	}
	panic("unreachable!")
}
func printType(t DagNodeType) string {
	switch t {
	case Start:
		return "Start"
	case End:
		return "End"
	case Simple:
		return "Simple"
	case Choice:
		return "Choice"
	case FanOut:
		return "FanOut"
	case FanIn:
		return "FanIn"
	}
	return ""
}

func (p *Progress) IsCompleted() bool {
	for _, node := range p.DagNodes {
		if node.Status == Pending {
			return false
		}
	}
	return true

}

// NextNodes retrieves the next nodes to execute, that have the minimum group with state pending
func (p *Progress) NextNodes() ([]DagNodeId, error) {
	minPendingGroup := -1
	// find the min group with node pending
	for _, node := range p.DagNodes {
		if node.Status == Pending {
			minPendingGroup = node.Group
			break
		}
		if node.Status == Failed {
			return []DagNodeId{}, fmt.Errorf("the execution is failed ")
		}
	}
	// get all node Ids within that group
	nodeIds := make([]DagNodeId, 0)
	for _, node := range p.DagNodes {
		if node.Group == minPendingGroup && node.Status == Pending {
			nodeIds = append(nodeIds, node.Id)
		}
	}
	p.NextGroup = minPendingGroup
	return nodeIds, nil
}

// CompleteNode sets the progress status of the node with the id input to 'Completed'
func (p *Progress) CompleteNode(id DagNodeId) error {
	for _, node := range p.DagNodes {
		if node.Id == id {
			node.Status = Executed
			return nil
		}
	}
	return fmt.Errorf("no node to complete with id %s exists in the dag for request %s", id, p.ReqId)
}

func (p *Progress) SkipNode(id DagNodeId) error {
	for _, node := range p.DagNodes {
		if node.Id == id {
			node.Status = Skipped
			fmt.Printf("skipped node %s\n", id)
			return nil
		}
	}
	return fmt.Errorf("no node to skip with id %s exists in the dag for request %s", id, p.ReqId)
}

func (p *Progress) SkipAll(nodes []DagNode) error {
	for _, node := range nodes {
		err := p.SkipNode(node.GetId())
		if err != nil {
			return err
		}
	}
	return nil
}

func (p *Progress) FailNode(id DagNodeId) error {
	for _, node := range p.DagNodes {
		if node.Id == id {
			node.Status = Failed
			return nil
		}
	}
	return fmt.Errorf("no node to fail with id %s exists in the dag for request %s", id, p.ReqId)
}

func (p *Progress) GetInfo(nodeId DagNodeId) *DagNodeInfo {
	for _, node := range p.DagNodes {
		if node.Id == nodeId {
			return node
		}
	}
	return nil
}

func (p *Progress) GetGroup(nodeId DagNodeId) int {
	for _, node := range p.DagNodes {
		if node.Id == nodeId {
			return node.Group
		}
	}
	return -1
}

// moveEndNodeAtTheEnd moves the end node at the end of the list and sets its group accordingly
func moveEndNodeAtTheEnd(nodeInfos []*DagNodeInfo) []*DagNodeInfo {
	// move the endNode at the end of the list
	var endNodeInfo *DagNodeInfo
	// get index of end node to remove
	indexToRemove := -1
	maxGroup := 0
	for i, nodeInfo := range nodeInfos {
		if nodeInfo.Type == End {
			indexToRemove = i
			endNodeInfo = nodeInfo
			continue
		}
		if nodeInfo.Group > maxGroup {
			maxGroup = nodeInfo.Group
		}
	}
	if indexToRemove != -1 {
		// remove end node
		nodeInfos = append(nodeInfos[:indexToRemove], nodeInfos[indexToRemove+1:]...)
		// update endNode group
		endNodeInfo.Group = maxGroup + 1
		// append at the end of the visited node list
		nodeInfos = append(nodeInfos, endNodeInfo)
	}
	return nodeInfos
}

// InitProgressRecursive initialize the node list assigning a group to each node, so that we can know which nodes should run in parallel or is a choice branch
func InitProgressRecursive(reqId ReqId, dag *Dag) *Progress {
	nodeInfos := extractNodeInfo(dag, dag.Start, 0, make([]*DagNodeInfo, 0))
	nodeInfos = moveEndNodeAtTheEnd(nodeInfos)
	nodeInfos = reorder(nodeInfos)
	return &Progress{
		ReqId:     ReqId(reqId),
		DagNodes:  nodeInfos,
		NextGroup: 0,
	}
}

// popMinGroupAndBranchNode removes the node with minimum group and, in case of multiple nodes in the same group, minimum branch
func popMinGroupAndBranchNode(infos *[]*DagNodeInfo) *DagNodeInfo {
	// finding min group nodes
	minGroup := math.MaxInt
	var minGroupNodeInfo []*DagNodeInfo
	for _, info := range *infos {
		if info.Group < minGroup {
			minGroupNodeInfo = make([]*DagNodeInfo, 0)
			minGroup = info.Group
			minGroupNodeInfo = append(minGroupNodeInfo, info)
		}
		if info.Group == minGroup {
			minGroupNodeInfo = append(minGroupNodeInfo, info)
		}
	}
	minBranch := math.MaxInt // when there are ties
	var minGroupAndBranchNode *DagNodeInfo

	// finding min branch node from those of the minimum group
	for _, info := range minGroupNodeInfo {
		if info.Branch < minBranch {
			minBranch = info.Branch
			minGroupAndBranchNode = info
		}
	}

	// finding index to remove from starting list
	var indexToRemove int
	for i, info := range *infos {
		if info.Id == minGroupAndBranchNode.Id {
			indexToRemove = i
			break
		}
	}
	*infos = append((*infos)[:indexToRemove], (*infos)[indexToRemove+1:]...)
	return minGroupAndBranchNode
}

func reorder(infos []*DagNodeInfo) []*DagNodeInfo {
	reordered := make([]*DagNodeInfo, 0)
	fmt.Println(len(reordered))
	for len(infos) > 0 {
		next := popMinGroupAndBranchNode(&infos)
		reordered = append(reordered, next)
	}
	return reordered
}

func isNodeInfoPresent(node DagNodeId, infos []*DagNodeInfo) bool {
	isPresent := false
	for _, nodeInfo := range infos {
		if nodeInfo.Id == node {
			isPresent = true
			break
		}
	}
	return isPresent
}

// extractNodeInfo retrieves all needed information from nodes and sets node groups. It duplicates end nodes.
func extractNodeInfo(dag *Dag, node DagNode, group int, infos []*DagNodeInfo) []*DagNodeInfo {
	info := newNodeInfo(node, group)
	if !isNodeInfoPresent(node.GetId(), infos) {
		infos = append(infos, info)
	} else if n, ok := node.(*FanInNode); ok {
		for _, nodeInfo := range infos {
			if nodeInfo.Id == n.GetId() {
				nodeInfo.Group = group
				break
			}
		}
	}
	group++
	switch n := node.(type) {
	case *StartNode:
		startNode, _ := dag.Find(n.GetNext()[0])
		toAdd := extractNodeInfo(dag, startNode, group, infos)
		for _, add := range toAdd {
			if !isNodeInfoPresent(add.Id, infos) {
				infos = append(infos, add)
			}
		}
		return infos
	case *SimpleNode:
		simpleNode, _ := dag.Find(n.GetNext()[0])
		toAdd := extractNodeInfo(dag, simpleNode, group, infos)
		for _, add := range toAdd {
			if !isNodeInfoPresent(add.Id, infos) {
				infos = append(infos, add)
			}
		}
		return infos
	case *EndNode:
		return infos
	case *ChoiceNode:
		for _, alternativeId := range n.Alternatives {
			alternative, _ := dag.Find(alternativeId)
			toAdd := extractNodeInfo(dag, alternative, group, infos)
			for _, add := range toAdd {
				if !isNodeInfoPresent(add.Id, infos) {
					infos = append(infos, add)
				}
			}
		}
		return infos
	case *FanOutNode:
		for _, parallelBranchId := range n.GetNext() {
			parallelBranch, _ := dag.Find(parallelBranchId)
			toAdd := extractNodeInfo(dag, parallelBranch, group, infos)
			for _, add := range toAdd {
				if !isNodeInfoPresent(add.Id, infos) {
					infos = append(infos, add)
				}
			}
		}
		return infos
	case *FanInNode:
		fanInNode, _ := dag.Find(n.GetNext()[0])
		toAdd := extractNodeInfo(dag, fanInNode, group, infos)
		for _, add := range toAdd {
			if !isNodeInfoPresent(add.Id, infos) {
				infos = append(infos, add)
			}
		}
	}
	return infos
}

func (p *Progress) Print() {
	str := fmt.Sprintf("Progress for composition request %s - G = node group, B = node branch\n", p.ReqId)
	str += fmt.Sprintln("G. |B| Type   (        NodeID        ) - Status")
	str += fmt.Sprintln("-------------------------------------------------")
	for _, info := range p.DagNodes {
		str += fmt.Sprintf("%d. |%d| %-6s (%-22s) - %s\n", info.Group, info.Branch, printType(info.Type), info.Id, printStatus(info.Status))
	}
	fmt.Printf("%s", str)
}

func (p *Progress) String() string {
	dagNodes := "["
	for i, node := range p.DagNodes {
		dagNodes += string(node.Id)
		if i != len(p.DagNodes)-1 {
			dagNodes += ", "
		}
	}
	dagNodes += "]"

	return fmt.Sprintf(`Progress{
		ReqId:     %s,
		DagNodes:  %s,
		NextGroup: %d,
	}`, p.ReqId, dagNodes, p.NextGroup)
}

func (p *Progress) Equals(p2 *Progress) bool {
	for i := range p.DagNodes {
		if !p.DagNodes[i].Equals(p2.DagNodes[i]) {
			return false
		}
	}

	return p.ReqId == p2.ReqId && p.NextGroup == p2.NextGroup
}

// Update should be used by a completed node after its execution
//func Update(p *Progress, s DagNodeStatus, n string, next n) {
//	p.doneNodes++ // TODO: how to deal with choice nodes?
//}

// SaveProgress should be used by a completed node after its execution
func (cache *ProgressCache) SaveProgress(p *Progress) error {
	// TODO: Save always in cache and in ETCD
	cache.progresses[p.ReqId] = p
	return nil
}

// RetrieveProgress should be used by the next node to execute
func (cache *ProgressCache) RetrieveProgress(reqId ReqId) (*Progress, bool) {
	// TODO: Get from cache if exists, otherwise from ETCD
	// TODO: retrieve progress from ETCD
	progress, ok := cache.progresses[reqId]
	return progress, ok
}

func (cache *ProgressCache) DeleteProgress(reqId ReqId) {
	delete(cache.progresses, reqId)
}

func IsEmptyProgressCache() bool {
	return len(progressCache.progresses) == 0
}
