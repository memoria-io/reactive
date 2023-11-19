package io.memoria.reactive.testsuite.event;

import io.memoria.reactive.eventsourcing.EventMeta;
import io.memoria.reactive.eventsourcing.StateId;

public record Debited(EventMeta meta, StateId creditedAcc, long amount) implements AccountEvent {}
