package io.memoria.reactive.testsuite.eventsourcing.banking.domain.command;

import io.memoria.reactive.eventsourcing.CommandId;
import io.memoria.reactive.eventsourcing.StateId;

public record CloseAccount(CommandId commandId, StateId accountId, long timestamp) implements AccountCommand {}
