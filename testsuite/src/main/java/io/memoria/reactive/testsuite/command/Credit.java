package io.memoria.reactive.testsuite.command;

import io.memoria.reactive.eventsourcing.CommandMeta;
import io.memoria.reactive.eventsourcing.StateId;

public record Credit(CommandMeta meta, StateId debitedAcc, long amount) implements AccountCommand {}
