package io.memoria.reactive.testsuite.eventsourcing.banking.domain.command;

import io.memoria.atom.core.id.Id;

public record CloseAccount(Id commandId, Id accountId, long timestamp) implements AccountCommand {}
