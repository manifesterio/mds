package kafka

import (
	"github.com/manifesterio/mds/dst/actor"
)

// The assertions of this package. Each one is declared once here, as a value of
// its own kind (Always, Sometimes, Reachable, ...), and the code uses the value.
// The declaration puts the name in the catalog of dst/actor. A name is never
// built at run time, and it must not change: reports and replays use it.
var (
	backpressurePauseNonNegative            = actor.NewAlways("kafka: a backpressure pause has a non-negative duration")                                           //determinismcheck:ok catalog handle, set once at start
	batchCommittedAtMostOncePerSend         = actor.NewAlways("kafka: a batch is committed at most once per send")                                                 //determinismcheck:ok catalog handle, set once at start
	batchWithBadCRCAccepted                 = actor.NewUnreachable("kafka: a batch with a bad CRC is accepted")                                                    //determinismcheck:ok catalog handle, set once at start
	batchWithProducerIDAccepted             = actor.NewUnreachable("kafka: a batch with a producer id is accepted")                                                //determinismcheck:ok catalog handle, set once at start
	clientAckedOffsetsIncrease              = actor.NewAlways("kafka: a client's acked batches for a partition are at increasing offsets")                         //determinismcheck:ok catalog handle, set once at start
	committedRangeIsOneProducedBatch        = actor.NewAlways("kafka: a committed range is one whole produced batch")                                              //determinismcheck:ok catalog handle, set once at start
	committedSegmentWithinBatchCap          = actor.NewAlways("kafka: a committed segment holds at most MaxSegmentBatches batches")                                //determinismcheck:ok catalog handle, set once at start
	committedSegmentReportsNonNegative      = actor.NewAlways("kafka: a committed segment reports non-negative occupancy")                                         //determinismcheck:ok catalog handle, set once at start
	committedSegmentWithinPartitionCap      = actor.NewAlways("kafka: a committed segment spans at most MaxSegmentPartitions partitions")                          //determinismcheck:ok catalog handle, set once at start
	committedSegmentCommitCostFits          = actor.NewAlways("kafka: a committed segment's commit cost (1 + partitions) fits the store's commit item limit")      //determinismcheck:ok catalog handle, set once at start
	consumerNeverReadsPastHighWatermark     = actor.NewAlways("kafka: a consumer never reads past the high watermark")                                             //determinismcheck:ok catalog handle, set once at start
	consumerNeverReceivesCorruptBatch       = actor.NewAlways("kafka: a consumer never receives a corrupt batch")                                                  //determinismcheck:ok catalog handle, set once at start
	consumerReadsCommittedValue             = actor.NewAlways("kafka: a consumer reads the committed value at each offset")                                        //determinismcheck:ok catalog handle, set once at start
	consumerResumesWhereLastReadEnded       = actor.NewAlways("kafka: a consumer resumes exactly where its last read ended")                                       //determinismcheck:ok catalog handle, set once at start
	consumerSeesEveryOffsetInOrder          = actor.NewAlways("kafka: a consumer sees every offset in order, none skipped")                                        //determinismcheck:ok catalog handle, set once at start
	consumerFetchNeverOutOfRange            = actor.NewAlways("kafka: a consumer's fetch is never out of range")                                                   //determinismcheck:ok catalog handle, set once at start
	definitelyRejectedBatchNeverCommitted   = actor.NewAlways("kafka: a definitely rejected batch is never committed")                                             //determinismcheck:ok catalog handle, set once at start
	failedSegmentReportsNonNegative         = actor.NewAlways("kafka: a failed segment reports non-negative time and occupancy")                                   //determinismcheck:ok catalog handle, set once at start
	fetchInHealSucceeds                     = actor.NewAlways("kafka: a fetch invoked in Heal succeeds")                                                           //determinismcheck:ok catalog handle, set once at start
	fetchHighWatermarkNotAboveLog           = actor.NewAlways("kafka: a fetch never reports a high watermark above the committed log")                             //determinismcheck:ok catalog handle, set once at start
	produceInHealIsAcked                    = actor.NewAlwaysOnFailure("kafka: a produce still running in Heal is acked")                                          //determinismcheck:ok catalog handle, set once at start
	recordCommittedWhereProduced            = actor.NewAlways("kafka: a record is committed where and as it was produced")                                         //determinismcheck:ok catalog handle, set once at start
	sealedSegmentWithinBatchCap             = actor.NewAlways("kafka: a sealed segment holds at most MaxSegmentBatches batches")                                   //determinismcheck:ok catalog handle, set once at start
	sealedSegmentOvershootsBytesByLastBatch = actor.NewAlways("kafka: a sealed segment overshoots MaxSegmentBytes by at most its last batch")                      //determinismcheck:ok catalog handle, set once at start
	sealedSegmentWithinPartitionCap         = actor.NewAlways("kafka: a sealed segment spans at most MaxSegmentPartitions partitions")                             //determinismcheck:ok catalog handle, set once at start
	sealedSegmentCommitCostFits             = actor.NewAlways("kafka: a sealed segment's implied commit cost (1 + partitions) fits the store's commit item limit") //determinismcheck:ok catalog handle, set once at start
	segmentSealedWithinBatchWindow          = actor.NewAlways("kafka: a segment is sealed within the window of its first batch")                                   //determinismcheck:ok catalog handle, set once at start
	segmentPutTimeNonNegative               = actor.NewAlways("kafka: a segment's put time is non-negative and it took at least one attempt")                      //determinismcheck:ok catalog handle, set once at start
	storedBatchCommittedAtMostOnce          = actor.NewAlways("kafka: a stored batch is committed at most once")                                                   //determinismcheck:ok catalog handle, set once at start
	ackFactWellFormed                       = actor.NewAlways("kafka: ack fact is well formed")                                                                    //determinismcheck:ok catalog handle, set once at start
	ackedRangeInControlPlane                = actor.NewAlways("kafka: acked range is committed in the control plane")                                              //determinismcheck:ok catalog handle, set once at start
	ackedRecordAtAckedOffset                = actor.NewAlways("kafka: an acked record is in the log at its acked offset")                                          //determinismcheck:ok catalog handle, set once at start
	clientBatchBuilds                       = actor.NewUnreachable("kafka: client batch builds")                                                                   //determinismcheck:ok catalog handle, set once at start
	clientRequestEncodes                    = actor.NewUnreachable("kafka: client request encodes")                                                                //determinismcheck:ok catalog handle, set once at start
	committedBatchIntact                    = actor.NewAlways("kafka: committed batch is intact")                                                                  //determinismcheck:ok catalog handle, set once at start
	committedOffsetsContiguous              = actor.NewAlways("kafka: committed offsets are contiguous")                                                           //determinismcheck:ok catalog handle, set once at start
	committedRangeHoldsRecordCount          = actor.NewAlways("kafka: committed range holds its record count")                                                     //determinismcheck:ok catalog handle, set once at start
	committedRangeLiesInsideSegment         = actor.NewAlways("kafka: committed range lies inside its segment")                                                    //determinismcheck:ok catalog handle, set once at start
	committedSegmentExistsObjectStore       = actor.NewAlways("kafka: committed segment exists in the object store")                                               //determinismcheck:ok catalog handle, set once at start
	everyCommittedRecordProduced            = actor.NewAlways("kafka: every committed record was produced")                                                        //determinismcheck:ok catalog handle, set once at start
	everyConsumerCaughtUp                   = actor.NewAlways("kafka: every consumer caught up with the final log")                                                //determinismcheck:ok catalog handle, set once at start
	noOpPendingAtEnd                        = actor.NewAlways("kafka: no op pending at end")                                                                       //determinismcheck:ok catalog handle, set once at start
	batchRecordsKeepOrder                   = actor.NewAlways("kafka: records of a batch keep their order")                                                        //determinismcheck:ok catalog handle, set once at start
	segmentDurationsAddUp                   = actor.NewAlways("kafka: segment put + queue wait + commit equals its seal-to-commit total")                          //determinismcheck:ok catalog handle, set once at start
	segmentDurationsNonNegative             = actor.NewAlways("kafka: segment put, queue wait and commit durations are non-negative")                              //determinismcheck:ok catalog handle, set once at start
	campaignCommittedAnyRecords             = actor.NewSometimes("kafka: the campaign committed records")                                                          //determinismcheck:ok catalog handle, set once at start
	highWatermarkIsLogEnd                   = actor.NewAlways("kafka: the high watermark is the end of the committed log")                                         //determinismcheck:ok catalog handle, set once at start
	kernelAnnouncedHeal                     = actor.NewAlways("kafka: the kernel announced Heal")                                                                  //determinismcheck:ok catalog handle, set once at start
	workloadIssuedFetches                   = actor.NewSometimes("kafka: workload issued fetches")                                                                 //determinismcheck:ok catalog handle, set once at start
	workloadIssuedProduces                  = actor.NewSometimes("kafka: workload issued produces")                                                                //determinismcheck:ok catalog handle, set once at start
)
