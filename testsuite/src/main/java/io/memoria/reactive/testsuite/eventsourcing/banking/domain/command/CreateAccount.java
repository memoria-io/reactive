package io.memoria.reactive.testsuite.eventsourcing.banking.domain.command;

import io.memoria.reactive.eventsourcing.CommandId;
import io.memoria.reactive.eventsourcing.StateId;

public record CreateAccount(CommandId commandId, StateId accountId, long timestamp, String accountName, long balance)
        implements AccountCommand {}
