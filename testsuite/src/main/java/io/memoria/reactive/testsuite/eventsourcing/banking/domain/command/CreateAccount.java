package io.memoria.reactive.testsuite.eventsourcing.banking.domain.command;

import io.memoria.atom.core.id.Id;

public record CreateAccount(Id commandId, Id accountId, long timestamp, String accountName, long balance)
        implements AccountCommand {}