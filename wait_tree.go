package main

import "github.com/reusee/pr"

type GlobalWaitTree struct {
	*pr.WaitTree
}

func (_ Global) WaitTree() GlobalWaitTree {
	return GlobalWaitTree{
		WaitTree: pr.NewRootWaitTree(),
	}
}

type NodeWaitTree struct {
	*pr.WaitTree
}

func (_ NodeScope) WaitTree(
	global GlobalWaitTree,
) NodeWaitTree {
	return NodeWaitTree{
		WaitTree: pr.NewWaitTree(global.WaitTree),
	}
}
