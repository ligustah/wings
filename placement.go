package wings

import (
	"strings"

	"github.com/ligustah/durable_streams/dswire"
	"github.com/ligustah/wings/flow"
)

// coordinatorRole is the label value marking the coordinator broker, and the
// name of the taint that keeps the expendable tier off it.
const coordinatorRole = "coordinator"

// The internal coordination streams the durable-streams cluster creates itself;
// their names reach [streamPlacement] like any other.
const (
	internalTransactionState = "__transaction_state"
	internalConsumerGroups   = "__consumer_groups"
)

// coordinatorLabels and coordinatorTaints mark the coordinator as the durable
// tier: it carries the role label leadership anchors to, and a taint that
// excludes every stream not tolerating it — so the expendable tier lives on
// workers and never on the coordinator.
var (
	coordinatorLabels = map[string]string{"role": coordinatorRole}
	coordinatorTaints = []string{coordinatorRole}
)

// coordinatorLed keeps a copy of a stream on the coordinator and leads it there,
// while its other replicas still spread to workers. It tolerates the coordinator
// taint so the stream may live there at all.
var coordinatorLed = &dswire.StreamPlacement{
	Tolerate: []string{coordinatorRole},
	Lead:     map[string]string{"role": coordinatorRole},
}

// streamPlacement is the placement wings stamps on a stream by name. The durable
// tier — the coordinator's own state, run history, and the internal coordination
// streams, which must be written on the raft node — leads on the coordinator.
// Everything else is expendable per-worker data and gets none, so the coordinator
// taint keeps it on the workers, RF-replicated across them.
func streamPlacement(stream string) *dswire.StreamPlacement {
	if coordinatorLeads(stream) {
		return coordinatorLed
	}
	return nil
}

func coordinatorLeads(stream string) bool {
	switch stream {
	case journalStream, machineStream, outputSet, internalTransactionState, internalConsumerGroups:
		return true
	}
	return strings.HasPrefix(stream, historyPrefix) ||
		strings.HasPrefix(stream, coordPrefix) ||
		strings.HasPrefix(stream, flow.ThreadStreamPrefix)
}
