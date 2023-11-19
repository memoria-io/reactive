package io.memoria.reactive.testsuite.event;

import io.memoria.atom.eventsourcing.EventMeta;
import io.memoria.atom.eventsourcing.StateId;

public record CreditRejected(EventMeta meta, StateId debitedAcc, long amount) implements AccountEvent {}
