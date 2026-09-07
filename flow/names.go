package flow

import (
	"runtime"
	"strconv"
	"sync"
)

// Build-time name inference. The wings build step compiles a call site → name
// table from the variable each [Define] is assigned to. A nameless Define cannot
// read the table when it runs (it is a var initialiser), so it records its call
// site and defers, resolved when the generated main calls
// [RegisterCallSiteNames]. Used without the build step, a nameless definition
// falls back to its call site at first use.

var (
	namesMu       sync.Mutex
	callSiteNames = map[string]string{}
)

// RegisterCallSiteNames supplies the call site → name table the wings build step
// compiled, and resolves every definition waiting for a name. The generated main
// calls it before it starts work. Keys are "<file>:<line>"; unknown entries are
// ignored, so one table serves a coordinator and a worker. Additive across calls.
func RegisterCallSiteNames(table map[string]string) {
	namesMu.Lock()
	for site, name := range table {
		callSiteNames[site] = name
	}
	namesMu.Unlock()
	resolvePendingDefs(false)
	resolvePendingMains()
}

// ensureNamesResolved names every waiting definition, defaulting any the table
// does not cover to its call site. Called at first use, when no more names can
// arrive.
func ensureNamesResolved() {
	resolvePendingDefs(true)
	resolvePendingMains()
}

// nameForSite is the name compiled for a call site, or "" when none was.
func nameForSite(site string) string {
	namesMu.Lock()
	defer namesMu.Unlock()
	return callSiteNames[site]
}

// callSite is the "<file>:<line>" of the caller skip frames up from the caller
// of this function, or "" when it cannot be recovered.
func callSite(skip int) string {
	_, file, line, ok := runtime.Caller(skip + 1)
	if !ok {
		return ""
	}
	return siteKey(file, line)
}

// siteKey is the table key for a file and line. Base name, not path, so it
// matches whether or not the binary was built with -trimpath.
func siteKey(file string, line int) string {
	return baseName(file) + ":" + strconv.Itoa(line)
}

// baseName is the last path element of file, handling both separators so a
// runtime.Caller path (always "/") and a build-time path agree.
func baseName(file string) string {
	for i := len(file) - 1; i >= 0; i-- {
		if file[i] == '/' || file[i] == '\\' {
			return file[i+1:]
		}
	}
	return file
}
