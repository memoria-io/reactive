package io.memoria.reactive.testsuite.event;

import io.memoria.atom.eventsourcing.EventMeta;

public record ClosureRejected(EventMeta meta) implements AccountEvent {}
