package dag

import (
	"testing"

	"github.com/flowforge/flowforge/pkg/domain"
)

func TestBuildGraph_ValidDiamond(t *testing.T) {
	// Diamond Graph:
	//       (A)
	//      /   \
	//    (B)   (C)
	//      \   /
	//       (D)
	def := domain.WorkflowDefinition{
		ID:   "diamond-wf",
		Name: "Diamond Workflow",
		Nodes: map[string]domain.Node{
			"A": {ID: "A", Type: "http"},
			"B": {ID: "B", Type: "http"},
			"C": {ID: "C", Type: "http"},
			"D": {ID: "D", Type: "http"},
		},
		Edges: []domain.Edge{
			{From: "A", To: "B"},
			{From: "A", To: "C"},
			{From: "B", To: "D"},
			{From: "C", To: "D"},
		},
	}

	g, err := BuildGraph(def)
	if err != nil {
		t.Fatalf("unexpected error building graph: %v", err)
	}

	roots := g.GetRoots()
	if len(roots) != 1 || roots[0] != "A" {
		t.Errorf("expected roots to be [A], got %v", roots)
	}

	if g.InDegree["A"] != 0 || g.InDegree["B"] != 1 || g.InDegree["C"] != 1 || g.InDegree["D"] != 2 {
		t.Errorf("incorrect in-degrees: %v", g.InDegree)
	}

	// Validate topological order / cycle check
	order, err := g.TopologicalSort()
	if err != nil {
		t.Fatalf("expected acyclic graph, got error: %v", err)
	}

	if len(order) != 4 {
		t.Errorf("expected 4 nodes in topological order, got %d (%v)", len(order), order)
	}

	// A must come before B and C, B and C before D
	indices := make(map[string]int)
	for i, node := range order {
		indices[node] = i
	}
	if indices["A"] > indices["B"] || indices["A"] > indices["C"] || indices["B"] > indices["D"] || indices["C"] > indices["D"] {
		t.Errorf("invalid topological order: %v", order)
	}
}

func TestBuildGraph_CycleDetection(t *testing.T) {
	// Circular graph: A -> B -> C -> A
	def := domain.WorkflowDefinition{
		ID:   "cyclic-wf",
		Name: "Cyclic Workflow",
		Nodes: map[string]domain.Node{
			"A": {ID: "A"},
			"B": {ID: "B"},
			"C": {ID: "C"},
		},
		Edges: []domain.Edge{
			{From: "A", To: "B"},
			{From: "B", To: "C"},
			{From: "C", To: "A"},
		},
	}

	g, err := BuildGraph(def)
	if err != nil {
		t.Fatalf("unexpected error building graph: %v", err)
	}

	_, err = g.TopologicalSort()
	if err != domain.ErrCycleDetected {
		t.Errorf("expected ErrCycleDetected, got %v", err)
	}
}
