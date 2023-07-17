package io.memoria.reactive.eventsourcing.testsuite.banking.domain.command;

import io.memoria.atom.core.id.Id;

public record ChangeName(Id commandId, Id accountId, long timestamp, String name) implements AccountCommand {}
