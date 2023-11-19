package io.memoria.reactive.testsuite.event;

import io.memoria.reactive.eventsourcing.EventMeta;

public record ClosureRejected(EventMeta meta) implements AccountEvent {}
