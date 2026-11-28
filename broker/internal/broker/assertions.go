package broker

import (
	"github.com/manifesterio/mds/dst/actor"
)

// The assertions of this package. Each one is declared once here, as a value of
// its own kind (Always, Sometimes, Reachable, ...), and the code uses the value.
// The declaration puts the name in the catalog of dst/actor. A name is never
// built at run time, and it must not change: reports and replays use it.
var (
	admittedBatchCannotEncode          = actor.NewUnreachable("broker: admitted batches cannot be encoded")              //determinismcheck:ok catalog handle, set once at start
	commitAnswersEveryBatchOrder       = actor.NewAlwaysOrUnreachable("broker: commit answers every batch in order")     //determinismcheck:ok catalog handle, set once at start
	commitDeadlineExceeded             = actor.NewReachableNotExpected("broker: commit deadline exceeded")               //determinismcheck:ok catalog handle, set once at start
	commitKeyReusedDifferentRequest    = actor.NewUnreachable("broker: commit key reused with a different request")      //determinismcheck:ok catalog handle, set once at start
	commitRetried                      = actor.NewReachable("broker: commit retried")                                    //determinismcheck:ok catalog handle, set once at start
	commitSucceededAfterOldRetryBudget = actor.NewReachable("broker: commit succeeded after the old retry budget")       //determinismcheck:ok catalog handle, set once at start
	committedSegmentMissing            = actor.NewUnreachable("broker: committed segment missing")                       //determinismcheck:ok catalog handle, set once at start
	corruptGetDetected                 = actor.NewReachable("broker: corrupt GET detected")                              //determinismcheck:ok catalog handle, set once at start
	corruptProduceBatchRejected        = actor.NewReachable("broker: corrupt produce batch rejected")                    //determinismcheck:ok catalog handle, set once at start
	fetchParkedForPoll                 = actor.NewReachable("broker: fetch parked for a poll")                           //determinismcheck:ok catalog handle, set once at start
	gzipBatchAdmitted                  = actor.NewReachable("broker: gzip batch admitted")                               //determinismcheck:ok catalog handle, set once at start
	idempotentBatchRejected            = actor.NewReachable("broker: idempotent batch rejected")                         //determinismcheck:ok catalog handle, set once at start
	lz4BatchAdmitted                   = actor.NewReachable("broker: lz4 batch admitted")                                //determinismcheck:ok catalog handle, set once at start
	oversizeBatchSealedAlone           = actor.NewReachable("broker: oversize batch sealed alone")                       //determinismcheck:ok catalog handle, set once at start
	readsPausedByBackpressure          = actor.NewReachable("broker: reads paused by backpressure")                      //determinismcheck:ok catalog handle, set once at start
	readsResumedAfterBackpressure      = actor.NewReachable("broker: reads resumed after backpressure")                  //determinismcheck:ok catalog handle, set once at start
	requestRetriesExhausted            = actor.NewReachable("broker: request retries exhausted")                         //determinismcheck:ok catalog handle, set once at start
	segmentExceedsCommitSize           = actor.NewUnreachable("broker: segment exceeds the control plane's commit size") //determinismcheck:ok catalog handle, set once at start
	segmentRetriesExhausted            = actor.NewReachable("broker: segment retries exhausted")                         //determinismcheck:ok catalog handle, set once at start
	segmentSealedBatchCap              = actor.NewReachable("broker: segment sealed by batch cap")                       //determinismcheck:ok catalog handle, set once at start
	segmentSealedPartitionCap          = actor.NewReachable("broker: segment sealed by partition cap")                   //determinismcheck:ok catalog handle, set once at start
	segmentSealedSize                  = actor.NewReachable("broker: segment sealed by size")                            //determinismcheck:ok catalog handle, set once at start
	segmentSealedTimer                 = actor.NewReachable("broker: segment sealed by timer")                           //determinismcheck:ok catalog handle, set once at start
	snappyBatchAdmitted                = actor.NewReachable("broker: snappy batch admitted")                             //determinismcheck:ok catalog handle, set once at start
	zstdBatchAdmitted                  = actor.NewReachable("broker: zstd batch admitted")                               //determinismcheck:ok catalog handle, set once at start
)
