package main

import (
	"fmt"

	"github.com/reusee/dscope"
	"github.com/reusee/e4"
)

var (
	he = e4.Handle
	ce = e4.Check.With(e4.WrapStacktrace)
	we = e4.Wrap.With(e4.WrapStacktrace)

	pt = fmt.Printf
)

type (
	Scope = dscope.Scope
)
