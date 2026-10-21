package chaos

import (
	"fmt"
	"maps"
	"slices"
)

// chain is the parent relation of a history: every snapshot that has a
// manifest, with its parent and its children. It answers the structural
// questions the checks ask, for Datasets and Volumes alike.
type chain[ID ~string] struct {
	parent   map[ID]ID
	children map[ID][]ID
}

func newChain[ID ~string]() *chain[ID] {
	return &chain[ID]{parent: make(map[ID]ID), children: make(map[ID][]ID)}
}

// add records snapshot id with its parent ("" for the first snapshot).
func (c *chain[ID]) add(id, parent ID) {
	c.parent[id] = parent
	if parent != "" {
		c.children[parent] = append(c.children[parent], id)
	}
}

func (c *chain[ID]) has(id ID) bool {
	_, ok := c.parent[id]
	return ok
}

func (c *chain[ID]) size() int { return len(c.parent) }

// ids lists the snapshots in ID order.
func (c *chain[ID]) ids() []ID { return slices.Sorted(maps.Keys(c.parent)) }

// forks lists the parents that have more than one child, in ID order.
func (c *chain[ID]) forks() []ID {
	var forks []ID
	for _, parent := range slices.Sorted(maps.Keys(c.children)) {
		if len(c.children[parent]) > 1 {
			forks = append(forks, parent)
		}
	}
	return forks
}

// missingParents lists the parents that have no manifest, in ID order.
func (c *chain[ID]) missingParents() []ID {
	var missing []ID
	for _, parent := range slices.Sorted(maps.Keys(c.children)) {
		if !c.has(parent) {
			missing = append(missing, parent)
		}
	}
	return missing
}

// ends lists the snapshots without a child, in ID order.
func (c *chain[ID]) ends() []ID {
	var ends []ID
	for _, id := range c.ids() {
		if len(c.children[id]) == 0 {
			ends = append(ends, id)
		}
	}
	return ends
}

// reach counts the snapshots reached by following parents from id, stopping
// at a parent without a manifest.
func (c *chain[ID]) reach(id ID) int {
	n := 0
	for ; c.has(id) && n <= c.size(); n++ {
		id = c.parent[id]
	}
	return n
}

// shape reports the structural violations of a history whose head is head:
// forks, more chain ends than missing parents account for, and, when no
// parent is missing, snapshots the head does not reach. A parent without a
// manifest is an abandoned commit or an expired snapshot (CONTRACT_CORE
// §Head Pointer, CONTRACT_RETENTION §Lineage); which of the two is checked
// by the caller, which knows what was acknowledged and expired.
func (c *chain[ID]) shape(head ID) []string {
	var problems []string
	for _, parent := range c.forks() {
		problems = append(problems, fmt.Sprintf("fork: %s has children %v", parent, c.children[parent]))
	}
	missing := c.missingParents()
	if ends := c.ends(); len(ends) > 1+len(missing) {
		problems = append(problems, fmt.Sprintf("chain ends %v exceed the head plus %d missing parents", ends, len(missing)))
	}
	if reached := c.reach(head); len(missing) == 0 && reached != c.size() {
		problems = append(problems, fmt.Sprintf("the head %s reaches %d of %d snapshots", head, reached, c.size()))
	}
	return problems
}
