package io.memoria.reactive.testsuite.event;

import io.memoria.reactive.eventsourcing.EventMeta;
import io.memoria.reactive.eventsourcing.StateId;

public record Credited(EventMeta meta, StateId debitedAcc, long amount) implements AccountEvent {}
