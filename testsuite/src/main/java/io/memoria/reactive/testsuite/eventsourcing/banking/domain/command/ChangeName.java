package io.memoria.reactive.testsuite.eventsourcing.banking.domain.command;

import io.memoria.atom.core.id.Id;

public record ChangeName(Id commandId, Id accountId, long timestamp, String name) implements AccountCommand {}
