package sql

func yieldNodes(yield func(Node) bool, nodes ...Node) bool {
	for _, n := range nodes {
		if n == nil || !n.node() {
			continue
		}

		if !yield(n) {
			return false
		}

		if !n.subnodes(yield) {
			return false
		}
	}

	return true
}

func Walk(n Node, yield func(Node) bool) {
	yieldNodes(yield, n)
}
