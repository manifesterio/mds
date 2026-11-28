package production

import (
	"log/slog"

	"github.com/manifesterio/mds/dst/actor"
)

// SegmentLog writes one structured log line per segment lifecycle event, built
// from the broker's facts so the broker core stays free of logging. Every
// segment fact names its segment by object key (field "segment"), so lines
// pair by key and not by order.
//
// Events (field "event"): segment_sealed, segment_put_done, segment_committed,
// segment_failed, backpressure_on, backpressure_off. Every segment line has
// segment, bytes, batches, partitions and, as they become known, reason,
// put_ms, wait_ms, commit_ms, total_ms. At benchmark rates (tens of segments a
// second) this is a few hundred lines a second at most; raise -log-level to
// warn to keep only failures.
//
// Observe must be called from one goroutine (the Facts hook).
type SegmentLog struct {
	log  *slog.Logger
	segs map[string]*segmentInfo
}

type segmentInfo struct {
	reason                     string
	bytes, batches, parts, age int64
	putAttempts                int64
	putMS                      float64
}

// NewSegmentLog returns a logger of segment lifecycle facts.
func NewSegmentLog(log *slog.Logger) *SegmentLog {
	return &SegmentLog{log: log, segs: map[string]*segmentInfo{}}
}

func ms(nanoseconds int64) float64 { return float64(nanoseconds/10_000) / 100 } // 10 us resolution

func fi(f actor.Fact, k string) int64 {
	switch x := f.Field(k).(type) {
	case actor.Int64Value:
		return int64(x)
	case actor.Uint64Value:
		return int64(x)
	case actor.StringValue, actor.Bool, actor.Bytes:
		return 0 // not a number
	}
	return 0
}

func fs(f actor.Fact, k string) string {
	s, _ := f.Field(k).(actor.StringValue)
	return string(s)
}

// Observe consumes one broker fact.
func (s *SegmentLog) Observe(f actor.Fact) {
	switch f.Name {
	case "segment_sealed":
		key := fs(f, "segment")
		si := &segmentInfo{reason: fs(f, "reason"), bytes: fi(f, "bytes"),
			batches: fi(f, "batches"), parts: fi(f, "parts"), age: fi(f, "age")}
		s.segs[key] = si
		s.log.Info("segment sealed", "event", "segment_sealed", "segment", key, "reason", si.reason, "bytes", si.bytes,
			"batches", si.batches, "partitions", si.parts, "open_age_ms", ms(si.age))
	case "segment_put_done":
		key := fs(f, "segment")
		si := s.segs[key]
		if si == nil {
			si = &segmentInfo{}
		}
		si.putAttempts, si.putMS = fi(f, "attempts"), ms(fi(f, "put_ns"))
		s.log.Info("segment put done", "event", "segment_put_done", "segment", key, "bytes", fi(f, "bytes"),
			"batches", si.batches, "partitions", si.parts, "put_ms", si.putMS, "put_attempts", si.putAttempts)
	case "segment_committed":
		key := fs(f, "segment")
		si := s.take(key)
		s.log.Info("segment committed", "event", "segment_committed", "segment", key, "reason", si.reason,
			"bytes", fi(f, "bytes"), "batches", fi(f, "batches"), "partitions", si.parts,
			"put_ms", ms(fi(f, "put_ns")), "wait_ms", ms(fi(f, "wait_ns")), "commit_ms", ms(fi(f, "commit_ns")),
			"total_ms", ms(fi(f, "total_ns")), "put_attempts", si.putAttempts,
			"in_flight", fi(f, "inflight"), "buffered_bytes", fi(f, "buffered"))
	case "create_topics_configs_ignored":
		s.log.Warn("CreateTopics configs ignored: mds keeps no topic configs", "event", "create_topics_configs_ignored",
			"topics", fi(f, "topics"))
	case "segment_failed":
		key := fs(f, "segment")
		si := s.take(key)
		s.log.Warn("segment failed", "event", "segment_failed", "segment", key, "reason", si.reason, "bytes", si.bytes,
			"batches", si.batches, "partitions", si.parts, "kafka_error_code", fi(f, "code"),
			"put_attempts", si.putAttempts, "put_ms", si.putMS, "total_ms", ms(fi(f, "total_ns")),
			"in_flight", fi(f, "inflight"), "buffered_bytes", fi(f, "buffered"))
	case "backpressure_on":
		s.log.Info("client reads paused", "event", "backpressure_on", "buffered_bytes", fi(f, "buffered"))
	case "backpressure_off":
		s.log.Info("client reads resumed", "event", "backpressure_off", "buffered_bytes", fi(f, "buffered"),
			"paused_ms", ms(fi(f, "paused_ns")))
	}
}

func (s *SegmentLog) take(key string) *segmentInfo {
	si := s.segs[key]
	if si == nil {
		return &segmentInfo{}
	}
	delete(s.segs, key)
	return si
}
