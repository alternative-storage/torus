package jaeger

import (
	//	"fmt"
	"time"

	"golang.org/x/net/context"

	"github.com/opentracing/opentracing-go"
	"github.com/uber/jaeger-client-go/config"
)

// Init creates a new instance of Jaeger tracer.
func Init(serviceName string) opentracing.Tracer {
	cfg := config.Configuration{
		Sampler: &config.SamplerConfig{
			Type:  "const",
			Param: 1,
		},
		Reporter: &config.ReporterConfig{
			LogSpans:            true,
			BufferFlushInterval: 1 * time.Second,
			QueueSize:           10,
		},
	}

	tracer, _, err := cfg.New(
		serviceName,
	)
	if err != nil {
		//		fmt.Fprintln(os.Stderr, err)
		//		os.Exit(1)
	}
	opentracing.SetGlobalTracer(tracer)
	return tracer
}

type tracer struct {
	Tr  opentracing.Tracer
	Ctx context.Context
}

var sharedInstance *tracer = &tracer{
	Tr:  Init("test"),
	Ctx: context.Background(),
}

func GetInstance() *tracer {
	return sharedInstance
}
