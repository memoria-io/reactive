package io.memoria.reactive.core.messaging.stream;

public record ESMsg(String topic, int partition, String key, String value) {}
