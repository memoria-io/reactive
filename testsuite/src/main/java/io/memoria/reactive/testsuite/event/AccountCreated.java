package io.memoria.reactive.testsuite.event;

import io.memoria.reactive.eventsourcing.EventMeta;

public record AccountCreated(EventMeta meta, String name, long balance) implements AccountEvent {}
