package com.example.streamix.core;

// Single domain exception type; factories keep call sites terse and messages consistent.
public class BrokerException extends RuntimeException {

	private final ErrorCode code;

	public BrokerException(ErrorCode code, String message) {
		super(message);
		this.code = code;
	}

	public ErrorCode code() { return code; }

	public static BrokerException unknownTopic(String topic) {
		return new BrokerException(ErrorCode.UNKNOWN_TOPIC, "topic '" + topic + "' does not exist");
	}

	public static BrokerException duplicateTopic(String topic) {
		return new BrokerException(ErrorCode.DUPLICATE_TOPIC, "topic '" + topic + "' already exists");
	}

	public static BrokerException unknownConsumer(String group, String consumerId) {
		return new BrokerException(ErrorCode.UNKNOWN_CONSUMER,
				"consumer '" + consumerId + "' is not registered in group '" + group + "' (sessions expire after their timeout; re-register)");
	}

	public static BrokerException duplicateConsumer(String group, String consumerId) {
		return new BrokerException(ErrorCode.DUPLICATE_CONSUMER,
				"consumer '" + consumerId + "' is already registered and alive in group '" + group + "'");
	}

	public static BrokerException partitionNotAssigned(String consumerId, String topic, int partition) {
		return new BrokerException(ErrorCode.PARTITION_NOT_ASSIGNED,
				"partition " + topic + "-" + partition + " is not assigned to consumer '" + consumerId + "' (group rebalanced?)");
	}

	public static BrokerException invalidOffset(String message) {
		return new BrokerException(ErrorCode.INVALID_OFFSET, message);
	}

	public static BrokerException invalidArgument(String message) {
		return new BrokerException(ErrorCode.INVALID_ARGUMENT, message);
	}

	public static BrokerException messageTooLarge(int size, int max) {
		return new BrokerException(ErrorCode.MESSAGE_TOO_LARGE, "message is " + size + " bytes, max is " + max);
	}
}
