// Package protos is the workflow event schema.
//
// It is the wire format of a workflow's event log and nothing else: no type in
// here appears in an exported signature of [github.com/ligustah/wings/flow].
// That is the same discipline the rest of wings applies to durable streams —
// the format is an implementation detail, and a user who never wants to see a
// protobuf never has to.
//
// Regenerate with `go generate ./flow/protos`. If protoc cannot find
// google/protobuf/timestamp.proto, pass its include directory explicitly with a
// second -I; some distributions do not put the well-known types on the default
// path.
package protos

//go:generate protoc -I ../.. --go_out=../.. --go_opt=module=github.com/ligustah/wings ../../flow/protos/data.proto ../../flow/protos/event.proto
