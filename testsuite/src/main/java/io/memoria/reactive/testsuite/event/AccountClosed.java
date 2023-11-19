package io.memoria.reactive.testsuite.event;

import io.memoria.atom.eventsourcing.EventMeta;

public record AccountClosed(EventMeta meta) implements AccountEvent {}
