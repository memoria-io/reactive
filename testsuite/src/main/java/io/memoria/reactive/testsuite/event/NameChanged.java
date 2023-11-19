package io.memoria.reactive.testsuite.event;

import io.memoria.reactive.eventsourcing.EventMeta;

public record NameChanged(EventMeta meta, String newName) implements AccountEvent {}
