/*
Copyright 2017 Google LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

Modifications:

- 2019, @john.koepi/@sitano extract pool, removed spanner specific code
*/

package spanner

import "container/heap"

var _ heap.Interface = (*hcHeap)(nil)

// hcHeap implements heap.Interface. It is used to create the priority queue for session healthchecks.
type hcHeap struct {
	sessions []*session
}

// Len implements heap.Interface.Len.
func (h hcHeap) Len() int {
	return len(h.sessions)
}

// Less implements heap.Interface.Less.
func (h hcHeap) Less(i, j int) bool {
	return h.sessions[i].getNextCheck().Before(h.sessions[j].getNextCheck())
}

// Swap implements heap.Interface.Swap.
func (h hcHeap) Swap(i, j int) {
	h.sessions[i], h.sessions[j] = h.sessions[j], h.sessions[i]
	h.sessions[i].setHcIndex(i)
	h.sessions[j].setHcIndex(j)
}

// Push implements heap.Interface.Push.
func (h *hcHeap) Push(s interface{}) {
	ns := s.(*session)
	ns.setHcIndex(len(h.sessions))
	h.sessions = append(h.sessions, ns)
}

// Pop implements heap.Interface.Pop.
func (h *hcHeap) Pop() interface{} {
	old := h.sessions
	n := len(old)
	s := old[n-1]
	h.sessions = old[:n-1]
	s.setHcIndex(-1)
	return s
}

