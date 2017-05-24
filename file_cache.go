package torus

import (
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/log"

	"golang.org/x/net/context"
)

type fileCache interface {
	newINode(ref INodeRef)
	writeToBlock(ctx context.Context, i, from, to int, data []byte, tracer opentracing.Tracer) (int, error)
	getBlock(ctx context.Context, i int, tracer opentracing.Tracer) ([]byte, error)
	sync(context.Context, opentracing.Tracer) error
}

type singleBlockCache struct {
	// half-finished blocks
	openIdx   int
	openData  []byte
	openWrote bool

	ref INodeRef

	blocks Blockset

	readIdx  int
	readData []byte

	blkSize uint64

	tracer opentracing.Tracer
}

func newSingleBlockCache(bs Blockset, blkSize uint64) *singleBlockCache {
	return &singleBlockCache{
		readIdx: -1,
		openIdx: -1,
		blocks:  bs,
		blkSize: blkSize,
	}
}

func (sb *singleBlockCache) newINode(ref INodeRef) {
	sb.ref = ref
}

func (sb *singleBlockCache) openBlock(ctx context.Context, i int) error {
	if sb.openIdx == i && sb.openData != nil {
		return nil
	}
	if sb.openWrote {
		// call sync
		err := sb.sync(ctx, sb.tracer)
		if err != nil {
			return err
		}
	}
	if i == sb.blocks.Length() {
		sb.openData = make([]byte, sb.blkSize)
		sb.openIdx = i
		return nil
	}
	if i > sb.blocks.Length() {
		panic("writing beyond the end of a file without calling Truncate")
	}

	if sb.readIdx == i {
		sb.openIdx = i
		sb.openData = sb.readData
		sb.readData = nil
		sb.readIdx = -1
		return nil
	}
	start := time.Now()
	d, err := sb.blocks.GetBlock(ctx, i, sb.tracer)
	if err != nil {
		return err
	}
	delta := time.Since(start)
	promFileBlockRead.Observe(float64(delta.Nanoseconds()) / 1000)
	sb.openData = d
	sb.openIdx = i
	return nil
}

func (sb *singleBlockCache) writeToBlock(ctx context.Context, i, from, to int, data []byte, tracer opentracing.Tracer) (int, error) {
	if span := opentracing.SpanFromContext(ctx); span != nil {
		span := tracer.StartSpan("write to memory", opentracing.ChildOf(span.Context()))
		span.SetTag("second", "write to memory")
		span.LogFields(
			log.String("writeToBlock", "writing..."))
		ctx = opentracing.ContextWithSpan(ctx, span)
		defer span.Finish()
	}
	sb.tracer = tracer
	if sb.openIdx != i {
		err := sb.openBlock(ctx, i)
		if err != nil {
			return 0, err
		}
	}
	sb.openWrote = true
	if (to - from) != len(data) {
		panic("server: different write lengths?")
	}
	return copy(sb.openData[from:to], data), nil
}

func (sb *singleBlockCache) sync(ctx context.Context, tracer opentracing.Tracer) error {
	if span := opentracing.SpanFromContext(ctx); span != nil {
		span := tracer.StartSpan("sync", opentracing.ChildOf(span.Context()))
		span.SetTag("syncing...", "syncing...")
		span.LogFields(
			log.String("writeToBlock", "writing..."))
		//log.Int("blkIndex", "TODO"))
		ctx = opentracing.ContextWithSpan(ctx, span)
		defer span.Finish()
	}

	if !sb.openWrote {
		return nil
	}
	start := time.Now()
	err := sb.blocks.PutBlock(ctx, sb.ref, sb.openIdx, sb.openData, tracer)
	delta := time.Since(start)
	promFileBlockWrite.Observe(float64(delta.Nanoseconds()) / 1000)
	sb.openWrote = false
	return err
}

// openRead gets the data gets the data from local or peers.
func (sb *singleBlockCache) openRead(ctx context.Context, i int, tracer opentracing.Tracer) error {
	if span := opentracing.SpanFromContext(ctx); span != nil {
		span := tracer.StartSpan("read data part 2", opentracing.ChildOf(span.Context()))
		ctx = opentracing.ContextWithSpan(ctx, span)
		defer span.Finish()
	}
	sb.tracer = tracer
	start := time.Now()
	d, err := sb.blocks.GetBlock(ctx, i, tracer)
	if err != nil {
		return err
	}
	delta := time.Since(start)
	promFileBlockRead.Observe(float64(delta.Nanoseconds()) / 1000)
	sb.readData = d
	sb.readIdx = i
	return nil
}

func (sb *singleBlockCache) getBlock(ctx context.Context, i int, tracer opentracing.Tracer) ([]byte, error) {
	if span := opentracing.SpanFromContext(ctx); span != nil {
		span := tracer.StartSpan("read data", opentracing.ChildOf(span.Context()))
		ctx = opentracing.ContextWithSpan(ctx, span)
		defer span.Finish()
	}
	sb.tracer = tracer

	if sb.openIdx == i {
		return sb.openData, nil
	}
	if sb.readIdx != i {
		err := sb.openRead(ctx, i, sb.tracer)
		if err != nil {
			return nil, err
		}
	}
	return sb.readData, nil
}
