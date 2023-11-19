package io.memoria.reactive.testsuite.command;

import io.memoria.atom.eventsourcing.CommandMeta;
import io.memoria.atom.eventsourcing.StateId;

public record ConfirmDebit(CommandMeta meta, StateId creditedAcc) implements AccountCommand {}
