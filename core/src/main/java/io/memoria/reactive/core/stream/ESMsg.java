package io.memoria.reactive.core.stream;

public record ESMsg(String topic, int partition, String key, String value) {}
