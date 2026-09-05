package protos

import (
	"fmt"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

var payloadOneof protoreflect.OneofDescriptor

func init() {
	payloadOneof = new(Event).ProtoReflect().Descriptor().Oneofs().ByName("payload")
}

// Events is every payload an [Event] can carry.
type Events interface {
	*CallEvent | *ForkEvent | *JoinEvent | *ReturnEvent | *SleepEvent | *GetTimeEvent |
		*RunStartEvent | *RunEndEvent | *ChannelSendEvent | *ChannelRecvEvent | *EffectEvent |
		*WaitInterruptedEvent
}

// PackEventPayload wraps a payload in its oneof case.
func PackEventPayload[EVENT Events](event EVENT) isEvent_Payload {
	switch e := any(event).(type) {
	case *CallEvent:
		return &Event_Call{Call: e}
	case *ForkEvent:
		return &Event_Fork{Fork: e}
	case *JoinEvent:
		return &Event_Join{Join: e}
	case *ReturnEvent:
		return &Event_Return{Return: e}
	case *SleepEvent:
		return &Event_Sleep{Sleep: e}
	case *GetTimeEvent:
		return &Event_Time{Time: e}
	case *RunStartEvent:
		return &Event_RunStart{RunStart: e}
	case *RunEndEvent:
		return &Event_RunEnd{RunEnd: e}
	case *ChannelSendEvent:
		return &Event_ChannelSend{ChannelSend: e}
	case *ChannelRecvEvent:
		return &Event_ChannelRecv{ChannelRecv: e}
	case *EffectEvent:
		return &Event_Effect{Effect: e}
	case *WaitInterruptedEvent:
		return &Event_Interrupted{Interrupted: e}
	default:
		panic(fmt.Sprintf("unknown event type: %T", event))
	}
}

// WhichPayload names the oneof case an event carries, or "<nil>".
func WhichPayload(event *Event) string {
	which := event.ProtoReflect().WhichOneof(payloadOneof)
	if which == nil {
		return "<nil>"
	}
	return string(which.Name())
}

// UnpackEventPayload returns the payload an event carries.
func UnpackEventPayload(event *Event) proto.Message {
	which := event.ProtoReflect().WhichOneof(payloadOneof)
	if which == nil {
		return nil
	}
	return event.ProtoReflect().Get(which).Message().Interface()
}

// EventType names the payload's message type, for diagnostics. A replay that
// finds the wrong kind of event reports with this, and the name is what tells
// somebody their workflow function changed shape between attempts.
func EventType(event *Event) string {
	payload := UnpackEventPayload(event)
	if payload == nil {
		return "<nil>"
	}
	return string(payload.ProtoReflect().Descriptor().Name())
}
