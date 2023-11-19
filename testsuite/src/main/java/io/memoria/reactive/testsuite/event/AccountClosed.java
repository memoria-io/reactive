package io.memoria.reactive.testsuite.event;

import io.memoria.reactive.eventsourcing.EventMeta;

public record AccountClosed(EventMeta meta) implements AccountEvent {}
