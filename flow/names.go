package flow

import (
	"runtime"
	"strconv"
	"sync"
)

// Build-time name inference.
//
// A function's name can come from [WithName], or be left to the `wings` build
// step, which reads the variable each [Define] is assigned to and compiles a
// table of call site → name into the program. A Define with no explicit name
// records its own call site — the file and line it was written on, off the
// call stack — and takes its name from that table.
//
// The table cannot be consulted when Define runs: a Define is a package-scope
// var initialiser, and those run before any generated code can register the
// table. So a nameless Define DEFERS: it records its call site and is resolved
// later, when [RegisterCallSiteNames] is called — which the generated main does
// before it starts any work, by when every var initialiser has run and the
// pending set is complete. A program that never calls it (the flow package used
// without `wings build`) leaves nameless definitions unresolved, and using one
// is an error naming its call site, so the fix — add WithName, or build with
// wings — is legible.

var (
	namesMu       sync.Mutex
	callSiteNames = map[string]string{}
)

// RegisterCallSiteNames supplies the call site → name table the `wings` build
// step compiled from your source, and resolves every definition still waiting
// for a name. The generated main calls it once, before it starts any work.
//
// Keys are "<file>:<line>", the base file name and the line a [Define] was
// written on, matching what [Define] reads off the call stack. Entries for
// definitions this process does not hold are ignored, so one table serves a
// coordinator and a worker built from different halves of the same source.
//
// Idempotent and additive: call it more than once and the tables merge.
func RegisterCallSiteNames(table map[string]string) {
	namesMu.Lock()
	for site, name := range table {
		callSiteNames[site] = name
	}
	namesMu.Unlock()
	resolvePendingDefs(false)
	resolvePendingMains()
}

// ensureNamesResolved names every definition still waiting, defaulting any the
// table does not cover to its own call site. Called at first use — when no more
// names can arrive — so a program that never calls [RegisterCallSiteNames] (the
// flow package used without `wings build`) still runs, each nameless definition
// identified by where it was written.
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

// siteKey is the table key for a file and line: the base file name and the
// line. The base name alone rather than the path, so it matches whether or not
// the binary was built with -trimpath, which rewrites the directory but never
// the file name.
func siteKey(file string, line int) string {
	return baseName(file) + ":" + strconv.Itoa(line)
}

// baseName is the last path element of file. Not path/filepath, because a path
// from runtime.Caller uses forward slashes on every platform while one read
// from the local filesystem at build time may use the OS separator; this
// handles both so the two ends agree.
func baseName(file string) string {
	for i := len(file) - 1; i >= 0; i-- {
		if file[i] == '/' || file[i] == '\\' {
			return file[i+1:]
		}
	}
	return file
}
