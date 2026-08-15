package com.example.streamix.group;

import com.example.streamix.core.Message;

public record PolledMessage(String topic, int partition, Message message) {
}
