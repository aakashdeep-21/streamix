package com.example.streamix.client;

import java.util.List;

// Throwing from handle() prevents the commit; the batch is redelivered (at-least-once).
@FunctionalInterface
public interface MessageHandler {

	void handle(List<ConsumedMessage> batch) throws Exception;
}
