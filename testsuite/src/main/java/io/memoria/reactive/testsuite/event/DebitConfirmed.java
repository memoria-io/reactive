package io.memoria.reactive.testsuite.event;

import io.memoria.atom.eventsourcing.EventMeta;

public record DebitConfirmed(EventMeta meta) implements AccountEvent {}
