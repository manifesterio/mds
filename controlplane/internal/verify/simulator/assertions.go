package simulator

import (
	"github.com/manifesterio/mds/dst/actor"
)

// The assertions of this package. Each one is declared once here, as a value of
// its own kind (Always, Sometimes, Reachable, ...), and the code uses the value.
// The declaration puts the name in the catalog of dst/actor. A name is never
// built at run time, and it must not change: reports and replays use it.
var (
	callAbandoned                 = actor.NewReachable("rebalance-sim: a call is abandoned")                          //determinismcheck:ok catalog handle, set once at start
	failedWriteEndsTerm           = actor.NewReachable("rebalance-sim: a failed write ends the term")                 //determinismcheck:ok catalog handle, set once at start
	heartbeatToldJoinAgain        = actor.NewReachable("rebalance-sim: a heartbeat is told to join again")            //determinismcheck:ok catalog handle, set once at start
	joinRoundCompletes            = actor.NewReachable("rebalance-sim: a join round completes")                       //determinismcheck:ok catalog handle, set once at start
	lateReplyAbandonedCallDropped = actor.NewReachable("rebalance-sim: a late reply to an abandoned call is dropped") //determinismcheck:ok catalog handle, set once at start
	leaveRemovesMember            = actor.NewReachable("rebalance-sim: a leave removes a member")                     //determinismcheck:ok catalog handle, set once at start
	memberGetsAssignment          = actor.NewReachable("rebalance-sim: a member gets its assignment")                 //determinismcheck:ok catalog handle, set once at start
	memberNeverSyncedRemoved      = actor.NewReachable("rebalance-sim: a member that never synced is removed")        //determinismcheck:ok catalog handle, set once at start
	refusedLoadReadSentAgain      = actor.NewReachable("rebalance-sim: a refused load read is sent again")            //determinismcheck:ok catalog handle, set once at start
	roundCompletesManyMembers     = actor.NewReachable("rebalance-sim: a round completes with many members")          //determinismcheck:ok catalog handle, set once at start
	silentMemberRemoved           = actor.NewReachable("rebalance-sim: a silent member is removed")                   //determinismcheck:ok catalog handle, set once at start
	takeoverLoadsGroupMembers     = actor.NewReachable("rebalance-sim: a takeover loads a group with members")        //determinismcheck:ok catalog handle, set once at start
	offsetCommitAcked             = actor.NewReachable("rebalance-sim: an offset commit is acked")                    //determinismcheck:ok catalog handle, set once at start
	groupsReadAgainFromFreshTail  = actor.NewReachable("rebalance-sim: groups are read again from a fresh tail")      //determinismcheck:ok catalog handle, set once at start
	roleLostWhileCallsWait        = actor.NewReachable("rebalance-sim: the role is lost while calls wait")            //determinismcheck:ok catalog handle, set once at start
	writeTimerEndsTerm            = actor.NewReachable("rebalance-sim: the write timer ends the term")                //determinismcheck:ok catalog handle, set once at start
	commitAnswerHasOneCode        = actor.NewAlways("sim: a commit answer has one code per offset")                   //determinismcheck:ok catalog handle, set once at start
	generationNeverGoesDown       = actor.NewAlwaysOnFailure("sim: a generation never goes down")                     //determinismcheck:ok catalog handle, set once at start
	knownMemberKeepsID            = actor.NewAlwaysOnFailure("sim: a known member keeps its id")                      //determinismcheck:ok catalog handle, set once at start
	machineAnswersOnlyWithRole    = actor.NewAlwaysOnFailure("sim: a machine answers only with the role")             //determinismcheck:ok catalog handle, set once at start
	silentMemberRemovedCheck      = actor.NewAlways("sim: a silent member is removed")                                //determinismcheck:ok catalog handle, set once at start
	syncAnswerIsLoggedAssignment  = actor.NewAlways("sim: a sync answer is the logged assignment")                    //determinismcheck:ok catalog handle, set once at start
	ackedWriteHasSpecAnswer       = actor.NewAlways("sim: acked write has the spec answer")                           //determinismcheck:ok catalog handle, set once at start
	ackedWriteInLog               = actor.NewAlways("sim: acked write is in the log")                                 //determinismcheck:ok catalog handle, set once at start
	ackedWriteVisible             = actor.NewAlwaysOnFailure("sim: acked write is visible")                           //determinismcheck:ok catalog handle, set once at start
	ackedOffsetCommitInLog        = actor.NewAlways("sim: an acked offset commit is in the log")                      //determinismcheck:ok catalog handle, set once at start
	ackedOffsetCommitSurvives     = actor.NewAlways("sim: an acked offset commit survives")                           //determinismcheck:ok catalog handle, set once at start
	answeredGenerationInLog       = actor.NewAlways("sim: an answered generation is in the log")                      //determinismcheck:ok catalog handle, set once at start
	consumerGroupsSettle          = actor.NewAlways("sim: consumer groups settle")                                    //determinismcheck:ok catalog handle, set once at start
	everyCallGetsAnswer           = actor.NewAlwaysOnFailure("sim: every call gets an answer")                        //determinismcheck:ok catalog handle, set once at start
	everyCallGetsOneAnswer        = actor.NewAlwaysOnFailure("sim: every call gets one answer")                       //determinismcheck:ok catalog handle, set once at start
	finalReadsAnswered            = actor.NewAlwaysOnFailure("sim: final reads are answered")                         //determinismcheck:ok catalog handle, set once at start
	liveReplicasConverge          = actor.NewAlways("sim: live replicas converge")                                    //determinismcheck:ok catalog handle, set once at start
	noTwoLeadersInGeneration      = actor.NewAlwaysOnFailure("sim: no two leaders of one generation")                 //determinismcheck:ok catalog handle, set once at start
	offsetsNeverGoBackwards       = actor.NewAlwaysOnFailure("sim: offsets never go backwards")                       //determinismcheck:ok catalog handle, set once at start
	oneWriterPerEpoch             = actor.NewAlwaysOnFailure("sim: one writer per epoch")                             //determinismcheck:ok catalog handle, set once at start
	readsAgreeWithSpec            = actor.NewAlways("sim: reads agree with the spec")                                 //determinismcheck:ok catalog handle, set once at start
	replicasAgreeEverySlot        = actor.NewAlwaysOnFailure("sim: replicas agree on every slot")                     //determinismcheck:ok catalog handle, set once at start
	runEnds                       = actor.NewAlwaysOnFailure("sim: run ends")                                         //determinismcheck:ok catalog handle, set once at start
	segmentListsNeverGoBackwards  = actor.NewAlwaysOnFailure("sim: segment lists never go backwards")                 //determinismcheck:ok catalog handle, set once at start
)

var (
	logHasNoHole     = actor.NewAlways("sim: log has no hole")    //determinismcheck:ok catalog handle, set once at start
	logEntriesDecode = actor.NewAlways("sim: log entries decode") //determinismcheck:ok catalog handle, set once at start
)
