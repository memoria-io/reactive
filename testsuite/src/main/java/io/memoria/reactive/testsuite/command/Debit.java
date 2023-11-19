package io.memoria.reactive.testsuite.command;

import io.memoria.reactive.eventsourcing.CommandMeta;
import io.memoria.reactive.eventsourcing.StateId;

public record Debit(CommandMeta meta, StateId creditedAcc, long amount) implements AccountCommand {}
