package io.memoria.reactive.eventsourcing.testsuite.banking.domain.command;

import io.memoria.atom.core.id.Id;

public record CloseAccount(Id commandId, Id accountId, long timestamp) implements AccountCommand {}
