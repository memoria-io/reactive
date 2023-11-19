package io.memoria.reactive.testsuite.event;

import io.memoria.reactive.eventsourcing.EventMeta;

public record DebitConfirmed(EventMeta meta) implements AccountEvent {}
