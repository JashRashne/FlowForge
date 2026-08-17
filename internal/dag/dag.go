package dag

import (
	"github.com/flowforge/flowforge/pkg/domain"
)

// Graph represents an in-memory directed dependency graph for a workflow.
type Graph struct {
	Nodes    map[string]domain.Node
	Parents  map[string][]string // Predecessors: Parents[node] = list of nodes that must finish BEFORE node
	Children map[string][]string // Dependents: Children[node] = list of nodes that run AFTER node
	InDegree map[string]int      // Number of incoming edges (dependencies) per node
}

// BuildGraph initializes and validates the adjacency relationships from a WorkflowDefinition.
func BuildGraph(def domain.WorkflowDefinition) (*Graph, error) {
	g := &Graph{
		Nodes:    make(map[string]domain.Node),
		Parents:  make(map[string][]string),
		Children: make(map[string][]string),
		InDegree: make(map[string]int),
	}

	// 1. Register all nodes
	for id, node := range def.Nodes {
		g.Nodes[id] = node
		g.Parents[id] = make([]string, 0)
		g.Children[id] = make([]string, 0)
		g.InDegree[id] = 0
	}

	// 2. Register all edges and validate that source and target nodes exist
	for _, edge := range def.Edges {
		if _, exists := g.Nodes[edge.From]; !exists {
			return nil, domain.ErrNodeNotFound
		}
		if _, exists := g.Nodes[edge.To]; !exists {
			return nil, domain.ErrNodeNotFound
		}

		g.Children[edge.From] = append(g.Children[edge.From], edge.To)
		g.Parents[edge.To] = append(g.Parents[edge.To], edge.From)
		g.InDegree[edge.To]++
	}

	return g, nil
}

// GetRoots returns all node IDs that have no dependencies (InDegree == 0).
// These are the nodes that are immediately READY when a workflow starts.
func (g *Graph) GetRoots() []string {
	var roots []string
	for id, degree := range g.InDegree {
		if degree == 0 {
			roots = append(roots, id)
		}
	}
	return roots
}

// TopologicalSort validates that the graph is Acyclic (contains no loops)
// and returns the nodes in a valid linear execution order using Kahn's Algorithm.
// If a cycle is detected, it must return domain.ErrCycleDetected.
func (g *Graph) TopologicalSort() ([]string, error) {
	// TODO: Implement Kahn's Algorithm
	inDegreeCopy := make(map[string]int)
	for k, v := range g.InDegree {
		inDegreeCopy[k] = v
	}

	var queue []string
	var result []string

	for k, v := range inDegreeCopy {
		if v == 0 {
			queue = append(queue, k)
		}
	}

	for len(queue) > 0 {
		curr := queue[0]
		queue = queue[1:]
		result = append(result, curr)
		for _, child := range g.Children[curr] {
			inDegreeCopy[child]--
			if inDegreeCopy[child] == 0 {
				queue = append(queue, child)
			}
		}
	}
	if len(result) != len(g.Nodes) {
		return nil, domain.ErrCycleDetected
	}
	return result, nil
}
