package io.memoria.reactive.testsuite.event;

import io.memoria.reactive.eventsourcing.EventMeta;

public record DebitRejected(EventMeta meta) implements AccountEvent {}
