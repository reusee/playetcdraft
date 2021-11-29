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

func (_ NodeScope) WaitTree(
	global GlobalWaitTree,
) *pr.WaitTree {
	return pr.NewWaitTree(global.WaitTree)
}
