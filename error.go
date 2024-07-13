package kafka

import (
	"errors"
	"fmt"
	"io"
	"syscall"
)

// KafkaError represents the different error codes that may be returned by kafka.
// https://kafka.apache.org/protocol#protocol_error_codes
type KafkaError int

const (
	Unknown                            KafkaError = -1
	OffsetOutOfRange                   KafkaError = 1
	InvalidMessage                     KafkaError = 2
	UnknownTopicOrPartition            KafkaError = 3
	InvalidMessageSize                 KafkaError = 4
	LeaderNotAvailable                 KafkaError = 5
	NotLeaderForPartition              KafkaError = 6
	RequestTimedOut                    KafkaError = 7
	BrokerNotAvailable                 KafkaError = 8
	ReplicaNotAvailable                KafkaError = 9
	MessageSizeTooLarge                KafkaError = 10
	StaleControllerEpoch               KafkaError = 11
	OffsetMetadataTooLarge             KafkaError = 12
	NetworkException                   KafkaError = 13
	GroupLoadInProgress                KafkaError = 14
	GroupCoordinatorNotAvailable       KafkaError = 15
	NotCoordinatorForGroup             KafkaError = 16
	InvalidTopic                       KafkaError = 17
	RecordListTooLarge                 KafkaError = 18
	NotEnoughReplicas                  KafkaError = 19
	NotEnoughReplicasAfterAppend       KafkaError = 20
	InvalidRequiredAcks                KafkaError = 21
	IllegalGeneration                  KafkaError = 22
	InconsistentGroupProtocol          KafkaError = 23
	InvalidGroupId                     KafkaError = 24
	UnknownMemberId                    KafkaError = 25
	InvalidSessionTimeout              KafkaError = 26
	RebalanceInProgress                KafkaError = 27
	InvalidCommitOffsetSize            KafkaError = 28
	TopicAuthorizationFailed           KafkaError = 29
	GroupAuthorizationFailed           KafkaError = 30
	ClusterAuthorizationFailed         KafkaError = 31
	InvalidTimestamp                   KafkaError = 32
	UnsupportedSASLMechanism           KafkaError = 33
	IllegalSASLState                   KafkaError = 34
	UnsupportedVersion                 KafkaError = 35
	TopicAlreadyExists                 KafkaError = 36
	InvalidPartitionNumber             KafkaError = 37
	InvalidReplicationFactor           KafkaError = 38
	InvalidReplicaAssignment           KafkaError = 39
	InvalidConfiguration               KafkaError = 40
	NotController                      KafkaError = 41
	InvalidRequest                     KafkaError = 42
	UnsupportedForMessageFormat        KafkaError = 43
	PolicyViolation                    KafkaError = 44
	OutOfOrderSequenceNumber           KafkaError = 45
	DuplicateSequenceNumber            KafkaError = 46
	InvalidProducerEpoch               KafkaError = 47
	InvalidTransactionState            KafkaError = 48
	InvalidProducerIDMapping           KafkaError = 49
	InvalidTransactionTimeout          KafkaError = 50
	ConcurrentTransactions             KafkaError = 51
	TransactionCoordinatorFenced       KafkaError = 52
	TransactionalIDAuthorizationFailed KafkaError = 53
	SecurityDisabled                   KafkaError = 54
	BrokerAuthorizationFailed          KafkaError = 55
	KafkaStorageError                  KafkaError = 56
	LogDirNotFound                     KafkaError = 57
	SASLAuthenticationFailed           KafkaError = 58
	UnknownProducerId                  KafkaError = 59
	ReassignmentInProgress             KafkaError = 60
	DelegationTokenAuthDisabled        KafkaError = 61
	DelegationTokenNotFound            KafkaError = 62
	DelegationTokenOwnerMismatch       KafkaError = 63
	DelegationTokenRequestNotAllowed   KafkaError = 64
	DelegationTokenAuthorizationFailed KafkaError = 65
	DelegationTokenExpired             KafkaError = 66
	InvalidPrincipalType               KafkaError = 67
	NonEmptyGroup                      KafkaError = 68
	GroupIdNotFound                    KafkaError = 69
	FetchSessionIDNotFound             KafkaError = 70
	InvalidFetchSessionEpoch           KafkaError = 71
	ListenerNotFound                   KafkaError = 72
	TopicDeletionDisabled              KafkaError = 73
	FencedLeaderEpoch                  KafkaError = 74
	UnknownLeaderEpoch                 KafkaError = 75
	UnsupportedCompressionType         KafkaError = 76
	StaleBrokerEpoch                   KafkaError = 77
	OffsetNotAvailable                 KafkaError = 78
	MemberIDRequired                   KafkaError = 79
	PreferredLeaderNotAvailable        KafkaError = 80
	GroupMaxSizeReached                KafkaError = 81
	FencedInstanceID                   KafkaError = 82
	EligibleLeadersNotAvailable        KafkaError = 83
	ElectionNotNeeded                  KafkaError = 84
	NoReassignmentInProgress           KafkaError = 85
	GroupSubscribedToTopic             KafkaError = 86
	InvalidRecord                      KafkaError = 87
	UnstableOffsetCommit               KafkaError = 88
	ThrottlingQuotaExceeded            KafkaError = 89
	ProducerFenced                     KafkaError = 90
	ResourceNotFound                   KafkaError = 91
	DuplicateResource                  KafkaError = 92
	UnacceptableCredential             KafkaError = 93
	InconsistentVoterSet               KafkaError = 94
	InvalidUpdateVersion               KafkaError = 95
	FeatureUpdateFailed                KafkaError = 96
	PrincipalDeserializationFailure    KafkaError = 97
	SnapshotNotFound                   KafkaError = 98
	PositionOutOfRange                 KafkaError = 99
	UnknownTopicID                     KafkaError = 100
	DuplicateBrokerRegistration        KafkaError = 101
	BrokerIDNotRegistered              KafkaError = 102
	InconsistentTopicID                KafkaError = 103
	InconsistentClusterID              KafkaError = 104
	TransactionalIDNotFound            KafkaError = 105
	FetchSessionTopicIDError           KafkaError = 106
)

// KafkaError satisfies the error interface.
func (e KafkaError) Error() string {
	return fmt.Sprintf("[%d] %s: %s", e, e.Title(), e.Description())
}

// Timeout returns true if the error was due to a timeout.
func (e KafkaError) Timeout() bool {
	return e == RequestTimedOut
}

// Temporary returns true if the operation that generated the error may succeed
// if retried at a later time.
// Kafka error documentation specifies these as "retriable"
// https://kafka.apache.org/protocol#protocol_error_codes
func (e KafkaError) Temporary() bool {
	switch e {
	case InvalidMessage,
		UnknownTopicOrPartition,
		LeaderNotAvailable,
		NotLeaderForPartition,
		RequestTimedOut,
		NetworkException,
		GroupLoadInProgress,
		GroupCoordinatorNotAvailable,
		NotCoordinatorForGroup,
		NotEnoughReplicas,
		NotEnoughReplicasAfterAppend,
		NotController,
		KafkaStorageError,
		FetchSessionIDNotFound,
		InvalidFetchSessionEpoch,
		ListenerNotFound,
		FencedLeaderEpoch,
		UnknownLeaderEpoch,
		OffsetNotAvailable,
		PreferredLeaderNotAvailable,
		EligibleLeadersNotAvailable,
		ElectionNotNeeded,
		NoReassignmentInProgress,
		GroupSubscribedToTopic,
		UnstableOffsetCommit,
		ThrottlingQuotaExceeded,
		UnknownTopicID,
		InconsistentTopicID,
		FetchSessionTopicIDError:
		return true
	default:
		return false
	}
}

// Title returns a human readable title for the error.
func (e KafkaError) Title() string {
	switch e {
	case Unknown:
		return "Unknown"
	case OffsetOutOfRange:
		return "Offset Out Of Range"
	case InvalidMessage:
		return "Invalid Message"
	case UnknownTopicOrPartition:
		return "Unknown Topic Or Partition"
	case InvalidMessageSize:
		return "Invalid Message Size"
	case LeaderNotAvailable:
		return "Leader Not Available"
	case NotLeaderForPartition:
		return "Not Leader For Partition"
	case RequestTimedOut:
		return "Request Timed Out"
	case BrokerNotAvailable:
		return "Broker Not Available"
	case ReplicaNotAvailable:
		return "Replica Not Available"
	case MessageSizeTooLarge:
		return "Message Size Too Large"
	case StaleControllerEpoch:
		return "Stale Controller Epoch"
	case OffsetMetadataTooLarge:
		return "Offset Metadata Too Large"
	case GroupLoadInProgress:
		return "Group Load In Progress"
	case GroupCoordinatorNotAvailable:
		return "Group Coordinator Not Available"
	case NotCoordinatorForGroup:
		return "Not Coordinator For Group"
	case InvalidTopic:
		return "Invalid Topic"
	case RecordListTooLarge:
		return "Record List Too Large"
	case NotEnoughReplicas:
		return "Not Enough Replicas"
	case NotEnoughReplicasAfterAppend:
		return "Not Enough Replicas After Append"
	case InvalidRequiredAcks:
		return "Invalid Required Acks"
	case IllegalGeneration:
		return "Illegal Generation"
	case InconsistentGroupProtocol:
		return "Inconsistent Group Protocol"
	case InvalidGroupId:
		return "Invalid Group ID"
	case UnknownMemberId:
		return "Unknown Member ID"
	case InvalidSessionTimeout:
		return "Invalid Session Timeout"
	case RebalanceInProgress:
		return "Rebalance In Progress"
	case InvalidCommitOffsetSize:
		return "Invalid Commit Offset Size"
	case TopicAuthorizationFailed:
		return "Topic Authorization Failed"
	case GroupAuthorizationFailed:
		return "Group Authorization Failed"
	case ClusterAuthorizationFailed:
		return "Cluster Authorization Failed"
	case InvalidTimestamp:
		return "Invalid Timestamp"
	case UnsupportedSASLMechanism:
		return "Unsupported SASL Mechanism"
	case IllegalSASLState:
		return "Illegal SASL State"
	case UnsupportedVersion:
		return "Unsupported Version"
	case TopicAlreadyExists:
		return "Topic Already Exists"
	case InvalidPartitionNumber:
		return "Invalid Partition Number"
	case InvalidReplicationFactor:
		return "Invalid Replication Factor"
	case InvalidReplicaAssignment:
		return "Invalid Replica Assignment"
	case InvalidConfiguration:
		return "Invalid Configuration"
	case NotController:
		return "Not Controller"
	case InvalidRequest:
		return "Invalid Request"
	case UnsupportedForMessageFormat:
		return "Unsupported For Message Format"
	case PolicyViolation:
		return "Policy Violation"
	case OutOfOrderSequenceNumber:
		return "Out Of Order Sequence Number"
	case DuplicateSequenceNumber:
		return "Duplicate Sequence Number"
	case InvalidProducerEpoch:
		return "Invalid Producer Epoch"
	case InvalidTransactionState:
		return "Invalid Transaction State"
	case InvalidProducerIDMapping:
		return "Invalid Producer ID Mapping"
	case InvalidTransactionTimeout:
		return "Invalid Transaction Timeout"
	case ConcurrentTransactions:
		return "Concurrent Transactions"
	case TransactionCoordinatorFenced:
		return "Transaction Coordinator Fenced"
	case TransactionalIDAuthorizationFailed:
		return "Transactional ID Authorization Failed"
	case SecurityDisabled:
		return "Security Disabled"
	case BrokerAuthorizationFailed:
		return "Broker Authorization Failed"
	case KafkaStorageError:
		return "Kafka Storage KafkaError"
	case LogDirNotFound:
		return "Log Dir Not Found"
	case SASLAuthenticationFailed:
		return "SASL Authentication Failed"
	case UnknownProducerId:
		return "Unknown Producer ID"
	case ReassignmentInProgress:
		return "Reassignment In Progress"
	case DelegationTokenAuthDisabled:
		return "Delegation Token Auth Disabled"
	case DelegationTokenNotFound:
		return "Delegation Token Not Found"
	case DelegationTokenOwnerMismatch:
		return "Delegation Token Owner Mismatch"
	case DelegationTokenRequestNotAllowed:
		return "Delegation Token Request Not Allowed"
	case DelegationTokenAuthorizationFailed:
		return "Delegation Token Authorization Failed"
	case DelegationTokenExpired:
		return "Delegation Token Expired"
	case InvalidPrincipalType:
		return "Invalid Principal Type"
	case NonEmptyGroup:
		return "Non Empty Group"
	case GroupIdNotFound:
		return "Group ID Not Found"
	case FetchSessionIDNotFound:
		return "Fetch Session ID Not Found"
	case InvalidFetchSessionEpoch:
		return "Invalid Fetch Session Epoch"
	case ListenerNotFound:
		return "Listener Not Found"
	case TopicDeletionDisabled:
		return "Topic Deletion Disabled"
	case FencedLeaderEpoch:
		return "Fenced Leader Epoch"
	case UnknownLeaderEpoch:
		return "Unknown Leader Epoch"
	case UnsupportedCompressionType:
		return "Unsupported Compression Type"
	case MemberIDRequired:
		return "Member ID Required"
	case EligibleLeadersNotAvailable:
		return "Eligible Leader Not Available"
	case ElectionNotNeeded:
		return "Election Not Needed"
	case NoReassignmentInProgress:
		return "No Reassignment In Progress"
	case GroupSubscribedToTopic:
		return "Group Subscribed To Topic"
	case InvalidRecord:
		return "Invalid Record"
	case UnstableOffsetCommit:
		return "Unstable Offset Commit"
	case ThrottlingQuotaExceeded:
		return "Throttling Quota Exceeded"
	case ProducerFenced:
		return "Producer Fenced"
	case ResourceNotFound:
		return "Resource Not Found"
	case DuplicateResource:
		return "Duplicate Resource"
	case UnacceptableCredential:
		return "Unacceptable Credential"
	case InconsistentVoterSet:
		return "Inconsistent Voter Set"
	case InvalidUpdateVersion:
		return "Invalid Update Version"
	case FeatureUpdateFailed:
		return "Feature Update Failed"
	case PrincipalDeserializationFailure:
		return "Principal Deserialization Failure"
	case SnapshotNotFound:
		return "Snapshot Not Found"
	case PositionOutOfRange:
		return "Position Out Of Range"
	case UnknownTopicID:
		return "Unknown Topic ID"
	case DuplicateBrokerRegistration:
		return "Duplicate Broker Registration"
	case BrokerIDNotRegistered:
		return "Broker ID Not Registered"
	case InconsistentTopicID:
		return "Inconsistent Topic ID"
	case InconsistentClusterID:
		return "Inconsistent Cluster ID"
	case TransactionalIDNotFound:
		return "Transactional ID Not Found"
	case FetchSessionTopicIDError:
		return "Fetch Session Topic ID KafkaError"
	}
	return ""
}

// Description returns a human readable description of cause of the error.
func (e KafkaError) Description() string {
	switch e {
	case Unknown:
		return "an unexpected server error occurred"
	case OffsetOutOfRange:
		return "the requested offset is outside the range of offsets maintained by the server for the given topic/partition"
	case InvalidMessage:
		return "the message contents does not match its CRC"
	case UnknownTopicOrPartition:
		return "the request is for a topic or partition that does not exist on this broker"
	case InvalidMessageSize:
		return "the message has a negative size"
	case LeaderNotAvailable:
		return "the cluster is in the middle of a leadership election and there is currently no leader for this partition and hence it is unavailable for writes"
	case NotLeaderForPartition:
		return "the client attempted to send messages to a replica that is not the leader for some partition, the client's metadata are likely out of date"
	case RequestTimedOut:
		return "the request exceeded the user-specified time limit in the request"
	case BrokerNotAvailable:
		return "not a client facing error and is used mostly by tools when a broker is not alive"
	case ReplicaNotAvailable:
		return "a replica is expected on a broker, but is not (this can be safely ignored)"
	case MessageSizeTooLarge:
		return "the server has a configurable maximum message size to avoid unbounded memory allocation and the client attempted to produce a message larger than this maximum"
	case StaleControllerEpoch:
		return "internal error code for broker-to-broker communication"
	case OffsetMetadataTooLarge:
		return "the client specified a string larger than configured maximum for offset metadata"
	case GroupLoadInProgress:
		return "the broker returns this error code for an offset fetch request if it is still loading offsets (after a leader change for that offsets topic partition), or in response to group membership requests (such as heartbeats) when group metadata is being loaded by the coordinator"
	case GroupCoordinatorNotAvailable:
		return "the broker returns this error code for group coordinator requests, offset commits, and most group management requests if the offsets topic has not yet been created, or if the group coordinator is not active"
	case NotCoordinatorForGroup:
		return "the broker returns this error code if it receives an offset fetch or commit request for a group that it is not a coordinator for"
	case InvalidTopic:
		return "a request which attempted to access an invalid topic (e.g. one which has an illegal name), or if an attempt was made to write to an internal topic (such as the consumer offsets topic)"
	case RecordListTooLarge:
		return "a message batch in a produce request exceeds the maximum configured segment size"
	case NotEnoughReplicas:
		return "the number of in-sync replicas is lower than the configured minimum and requiredAcks is -1"
	case NotEnoughReplicasAfterAppend:
		return "the message was written to the log, but with fewer in-sync replicas than required."
	case InvalidRequiredAcks:
		return "the requested requiredAcks is invalid (anything other than -1, 1, or 0)"
	case IllegalGeneration:
		return "the generation id provided in the request is not the current generation"
	case InconsistentGroupProtocol:
		return "the member provided a protocol type or set of protocols which is not compatible with the current group"
	case InvalidGroupId:
		return "the group id is empty or null"
	case UnknownMemberId:
		return "the member id is not in the current generation"
	case InvalidSessionTimeout:
		return "the requested session timeout is outside of the allowed range on the broker"
	case RebalanceInProgress:
		return "the coordinator has begun rebalancing the group, the client should rejoin the group"
	case InvalidCommitOffsetSize:
		return "an offset commit was rejected because of oversize metadata"
	case TopicAuthorizationFailed:
		return "the client is not authorized to access the requested topic"
	case GroupAuthorizationFailed:
		return "the client is not authorized to access a particular group id"
	case ClusterAuthorizationFailed:
		return "the client is not authorized to use an inter-broker or administrative API"
	case InvalidTimestamp:
		return "the timestamp of the message is out of acceptable range"
	case UnsupportedSASLMechanism:
		return "the broker does not support the requested SASL mechanism"
	case IllegalSASLState:
		return "the request is not valid given the current SASL state"
	case UnsupportedVersion:
		return "the version of API is not supported"
	case TopicAlreadyExists:
		return "a topic with this name already exists"
	case InvalidPartitionNumber:
		return "the number of partitions is invalid"
	case InvalidReplicationFactor:
		return "the replication-factor is invalid"
	case InvalidReplicaAssignment:
		return "the replica assignment is invalid"
	case InvalidConfiguration:
		return "the configuration is invalid"
	case NotController:
		return "this is not the correct controller for this cluster"
	case InvalidRequest:
		return "this most likely occurs because of a request being malformed by the client library or the message was sent to an incompatible broker, se the broker logs for more details"
	case UnsupportedForMessageFormat:
		return "the message format version on the broker does not support the request"
	case PolicyViolation:
		return "the request parameters do not satisfy the configured policy"
	case OutOfOrderSequenceNumber:
		return "the broker received an out of order sequence number"
	case DuplicateSequenceNumber:
		return "the broker received a duplicate sequence number"
	case InvalidProducerEpoch:
		return "the producer attempted an operation with an old epoch, either there is a newer producer with the same transactional ID, or the producer's transaction has been expired by the broker"
	case InvalidTransactionState:
		return "the producer attempted a transactional operation in an invalid state"
	case InvalidProducerIDMapping:
		return "the producer attempted to use a producer id which is not currently assigned to its transactional ID"
	case InvalidTransactionTimeout:
		return "the transaction timeout is larger than the maximum value allowed by the broker (as configured by max.transaction.timeout.ms)"
	case ConcurrentTransactions:
		return "the producer attempted to update a transaction while another concurrent operation on the same transaction was ongoing"
	case TransactionCoordinatorFenced:
		return "the transaction coordinator sending a WriteTxnMarker is no longer the current coordinator for a given producer"
	case TransactionalIDAuthorizationFailed:
		return "the transactional ID authorization failed"
	case SecurityDisabled:
		return "the security features are disabled"
	case BrokerAuthorizationFailed:
		return "the broker authorization failed"
	case KafkaStorageError:
		return "disk error when trying to access log file on the disk"
	case LogDirNotFound:
		return "the user-specified log directory is not found in the broker config"
	case SASLAuthenticationFailed:
		return "SASL Authentication failed"
	case UnknownProducerId:
		return "the broker could not locate the producer metadata associated with the producer ID"
	case ReassignmentInProgress:
		return "a partition reassignment is in progress"
	case DelegationTokenAuthDisabled:
		return "delegation token feature is not enabled"
	case DelegationTokenNotFound:
		return "delegation token is not found on server"
	case DelegationTokenOwnerMismatch:
		return "specified principal is not valid owner/renewer"
	case DelegationTokenRequestNotAllowed:
		return "delegation token requests are not allowed on plaintext/1-way ssl channels and on delegation token authenticated channels"
	case DelegationTokenAuthorizationFailed:
		return "delegation token authorization failed"
	case DelegationTokenExpired:
		return "delegation token is expired"
	case InvalidPrincipalType:
		return "supplied principaltype is not supported"
	case NonEmptyGroup:
		return "the group is not empty"
	case GroupIdNotFound:
		return "the group ID does not exist"
	case FetchSessionIDNotFound:
		return "the fetch session ID was not found"
	case InvalidFetchSessionEpoch:
		return "the fetch session epoch is invalid"
	case ListenerNotFound:
		return "there is no listener on the leader broker that matches the listener on which metadata request was processed"
	case TopicDeletionDisabled:
		return "topic deletion is disabled"
	case FencedLeaderEpoch:
		return "the leader epoch in the request is older than the epoch on the broker"
	case UnknownLeaderEpoch:
		return "the leader epoch in the request is newer than the epoch on the broker"
	case UnsupportedCompressionType:
		return "the requesting client does not support the compression type of given partition"
	case MemberIDRequired:
		return "the group member needs to have a valid member id before actually entering a consumer group"
	case EligibleLeadersNotAvailable:
		return "eligible topic partition leaders are not available"
	case ElectionNotNeeded:
		return "leader election not needed for topic partition"
	case NoReassignmentInProgress:
		return "no partition reassignment is in progress"
	case GroupSubscribedToTopic:
		return "deleting offsets of a topic is forbidden while the consumer group is actively subscribed to it"
	case InvalidRecord:
		return "this record has failed the validation on broker and hence be rejected"
	case UnstableOffsetCommit:
		return "there are unstable offsets that need to be cleared"
	case ThrottlingQuotaExceeded:
		return "The throttling quota has been exceeded"
	case ProducerFenced:
		return "There is a newer producer with the same transactionalId which fences the current one"
	case ResourceNotFound:
		return "A request illegally referred to a resource that does not exist"
	case DuplicateResource:
		return "A request illegally referred to the same resource twice"
	case UnacceptableCredential:
		return "Requested credential would not meet criteria for acceptability"
	case InconsistentVoterSet:
		return "Indicates that the either the sender or recipient of a voter-only request is not one of the expected voters"
	case InvalidUpdateVersion:
		return "The given update version was invalid"
	case FeatureUpdateFailed:
		return "Unable to update finalized features due to an unexpected server error"
	case PrincipalDeserializationFailure:
		return "Request principal deserialization failed during forwarding. This indicates an internal error on the broker cluster security setup"
	case SnapshotNotFound:
		return "Requested snapshot was not found"
	case PositionOutOfRange:
		return "Requested position is not greater than or equal to zero, and less than the size of the snapshot"
	case UnknownTopicID:
		return "This server does not host this topic ID"
	case DuplicateBrokerRegistration:
		return "This broker ID is already in use"
	case BrokerIDNotRegistered:
		return "The given broker ID was not registered"
	case InconsistentTopicID:
		return "The log's topic ID did not match the topic ID in the request"
	case InconsistentClusterID:
		return "The clusterId in the request does not match that found on the server"
	case TransactionalIDNotFound:
		return "The transactionalId could not be found"
	case FetchSessionTopicIDError:
		return "The fetch session encountered inconsistent topic ID usage"
	}
	return ""
}

func isTimeout(err error) bool {
	var timeoutError interface{ Timeout() bool }
	if errors.As(err, &timeoutError) {
		return timeoutError.Timeout()
	}
	return false
}

func isTemporary(err error) bool {
	var tempError interface{ Temporary() bool }
	if errors.As(err, &tempError) {
		return tempError.Temporary()
	}
	return false
}

func isTransientNetworkError(err error) bool {
	return errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, syscall.ECONNREFUSED) ||
		errors.Is(err, syscall.ECONNRESET) ||
		errors.Is(err, syscall.EPIPE)
}

func silentEOF(err error) error {
	if errors.Is(err, io.EOF) {
		err = nil
	}
	return err
}

func dontExpectEOF(err error) error {
	if errors.Is(err, io.EOF) {
		return io.ErrUnexpectedEOF
	}
	return err
}

func coalesceErrors(errs ...error) error {
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

type MessageTooLargeError struct {
	Message   Message
	Remaining []Message
}

func messageTooLarge(msgs []Message, i int) MessageTooLargeError {
	remain := make([]Message, 0, len(msgs)-1)
	remain = append(remain, msgs[:i]...)
	remain = append(remain, msgs[i+1:]...)
	return MessageTooLargeError{
		Message:   msgs[i],
		Remaining: remain,
	}
}

func (e MessageTooLargeError) Error() string {
	return MessageSizeTooLarge.Error()
}

type UnknownTopicOrPartitionError struct {
	KafkaError
	Topic     string
	Partition *int
}

func unknownTopicOrPartition(topic string, partition *int) UnknownTopicOrPartitionError {
	return UnknownTopicOrPartitionError{KafkaError: UnknownTopicOrPartition, Topic: topic, Partition: partition}
}

func (e UnknownTopicOrPartitionError) Is(err error) bool {
	if err == UnknownTopicOrPartition {
		return true
	}

	var unknownTopicOrPartitionError UnknownTopicOrPartitionError
	return errors.As(err, &unknownTopicOrPartitionError)
}

func makeError(code int16, message string) error {
	if code == 0 {
		return nil
	}
	if message == "" {
		return KafkaError(code)
	}
	return fmt.Errorf("%w: %s", KafkaError(code), message)
}

// WriteError is returned by kafka.(*Writer).WriteMessages when the writer is
// not configured to write messages asynchronously. WriteError values contain
// a list of errors where each entry matches the position of a message in the
// WriteMessages call. The program can determine the status of each message by
// looping over the error:
//
//	switch err := w.WriteMessages(ctx, msgs...).(type) {
//	case nil:
//	case kafka.WriteErrors:
//		for i := range msgs {
//			if err[i] != nil {
//				// handle the error writing msgs[i]
//				...
//			}
//		}
//	default:
//		// handle other errors
//		...
//	}
type WriteErrors []error

// Count counts the number of non-nil errors in err.
func (err WriteErrors) Count() int {
	n := 0

	for _, e := range err {
		if e != nil {
			n++
		}
	}

	return n
}

func (err WriteErrors) Error() string {
	errCount := err.Count()
	errors := make([]string, 0, errCount)
	for _, writeError := range err {
		if writeError == nil {
			continue
		}
		errors = append(errors, writeError.Error())
	}
	return fmt.Sprintf("Kafka write errors (%d/%d), errors: %v", errCount, len(err), errors)
}
