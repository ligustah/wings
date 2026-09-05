module github.com/ligustah/wings

go 1.27

toolchain go1.27.0

// durable_streams is not published to the module proxy, so every module of it
// is resolved from the working copy beside this one.
replace (
	github.com/ligustah/durable_streams => ../durable_streams
	github.com/ligustah/durable_streams/blobstore => ../durable_streams/blobstore
	github.com/ligustah/durable_streams/broker => ../durable_streams/broker
	github.com/ligustah/durable_streams/broker/client => ../durable_streams/broker/client
	github.com/ligustah/durable_streams/broker/protos => ../durable_streams/broker/protos
	github.com/ligustah/durable_streams/dsclient => ../durable_streams/dsclient
	github.com/ligustah/durable_streams/dsembedded => ../durable_streams/dsembedded
	github.com/ligustah/durable_streams/dswire => ../durable_streams/dswire
)

require (
	cloud.google.com/go/compute v1.67.0
	github.com/dave/jennifer v1.7.1
	github.com/ligustah/durable_streams v0.156.0
	github.com/ligustah/durable_streams/broker v0.241.0
	github.com/ligustah/durable_streams/broker/client v0.56.0
	github.com/ligustah/durable_streams/broker/protos v0.41.0
	github.com/ligustah/durable_streams/dsclient v0.51.0
	github.com/ligustah/durable_streams/dswire v0.33.0
	golang.org/x/crypto v0.55.0
	google.golang.org/api v0.287.1
	google.golang.org/grpc v1.83.2
	google.golang.org/protobuf v1.36.11
)

require (
	cloud.google.com/go/auth v0.20.0 // indirect
	cloud.google.com/go/auth/oauth2adapt v0.2.8 // indirect
	cloud.google.com/go/compute/metadata v0.9.0 // indirect
	github.com/armon/go-metrics v0.4.1 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/boltdb/bolt v1.3.1 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/fatih/color v1.13.0 // indirect
	github.com/felixge/httpsnoop v1.0.4 // indirect
	github.com/go-logr/logr v1.4.3 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/golang/snappy v1.0.0 // indirect
	github.com/google/s2a-go v0.1.9 // indirect
	github.com/googleapis/enterprise-certificate-proxy v0.3.17 // indirect
	github.com/googleapis/gax-go/v2 v2.23.0 // indirect
	github.com/hashicorp/go-hclog v1.6.2 // indirect
	github.com/hashicorp/go-immutable-radix v1.0.0 // indirect
	github.com/hashicorp/go-metrics v0.5.4 // indirect
	github.com/hashicorp/go-msgpack/v2 v2.1.2 // indirect
	github.com/hashicorp/golang-lru v0.5.1 // indirect
	github.com/hashicorp/raft v1.7.3 // indirect
	github.com/hashicorp/raft-boltdb/v2 v2.3.1 // indirect
	github.com/klauspost/compress v1.19.2 // indirect
	github.com/ligustah/commitlog v0.104.0 // indirect
	github.com/ligustah/durable_streams/dsembedded v0.79.0 // indirect
	github.com/mattn/go-colorable v0.1.12 // indirect
	github.com/mattn/go-isatty v0.0.14 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/natefinch/atomic v1.0.1 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/prometheus/client_golang v1.24.0 // indirect
	github.com/prometheus/client_model v0.6.2 // indirect
	github.com/prometheus/common v0.70.0 // indirect
	github.com/prometheus/procfs v0.21.1 // indirect
	github.com/tysonmote/gommap v0.0.3 // indirect
	go.etcd.io/bbolt v1.3.5 // indirect
	go.opentelemetry.io/auto/sdk v1.2.1 // indirect
	go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp v0.67.0 // indirect
	go.opentelemetry.io/otel v1.44.0 // indirect
	go.opentelemetry.io/otel/metric v1.44.0 // indirect
	go.opentelemetry.io/otel/trace v1.44.0 // indirect
	golang.org/x/net v0.58.0 // indirect
	golang.org/x/oauth2 v0.36.0 // indirect
	golang.org/x/sys v0.47.0 // indirect
	golang.org/x/text v0.41.0 // indirect
	google.golang.org/genproto v0.0.0-20260319201613-d00831a3d3e7 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20260630182238-925bb5da69e7 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260630182238-925bb5da69e7 // indirect
)
