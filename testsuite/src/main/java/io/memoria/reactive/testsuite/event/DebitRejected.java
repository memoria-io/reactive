package io.memoria.reactive.testsuite.event;

import io.memoria.atom.eventsourcing.EventMeta;

public record DebitRejected(EventMeta meta) implements AccountEvent {}
