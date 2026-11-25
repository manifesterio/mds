package main

func circles(edges [][]int) [][]int {
	const unseen = -1
	n := len(edges)
	index := make([]int, n)
	low := make([]int, n)
	onStack := make([]bool, n)
	for i := range index {
		index[i] = unseen
	}
	var stack []int
	next := 0
	var found [][]int

	type frame struct{ node, edge int }
	for start := range n {
		if index[start] != unseen {
			continue
		}
		work := []frame{{start, 0}}
		index[start] = next
		low[start] = next
		next++
		stack = append(stack, start)
		onStack[start] = true
		for len(work) > 0 {
			node, edge := work[len(work)-1].node, work[len(work)-1].edge
			if edge < len(edges[node]) {
				to := edges[node][edge]
				work[len(work)-1].edge++
				if index[to] == unseen {
					index[to] = next
					low[to] = next
					next++
					stack = append(stack, to)
					onStack[to] = true
					work = append(work, frame{to, 0})
				} else if onStack[to] {
					if index[to] < low[node] {
						low[node] = index[to]
					}
				}
				continue
			}
			work = work[:len(work)-1]
			if len(work) > 0 {
				parent := work[len(work)-1].node
				if low[node] < low[parent] {
					low[parent] = low[node]
				}
			}
			if low[node] == index[node] {
				var comp []int
				for {
					m := stack[len(stack)-1]
					stack = stack[:len(stack)-1]
					onStack[m] = false
					comp = append(comp, m)
					if m == node {
						break
					}
				}
				if len(comp) > 1 {
					sortInts(comp)
					found = append(found, comp)
				}
			}
		}
	}
	sortIntSlices(found)
	return found
}

func sortInts(a []int) {
	for i := 1; i < len(a); i++ {
		for j := i; j > 0 && a[j] < a[j-1]; j-- {
			a[j], a[j-1] = a[j-1], a[j]
		}
	}
}

func sortIntSlices(a [][]int) {
	for i := 1; i < len(a); i++ {
		for j := i; j > 0 && lessIntSlice(a[j], a[j-1]); j-- {
			a[j], a[j-1] = a[j-1], a[j]
		}
	}
}

func lessIntSlice(a, b []int) bool {
	for i := 0; i < len(a) && i < len(b); i++ {
		if a[i] != b[i] {
			return a[i] < b[i]
		}
	}
	return len(a) < len(b)
}
