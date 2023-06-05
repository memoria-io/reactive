package io.memoria.reactive.eventsourcing.usecase.banking.state;

import io.memoria.atom.core.id.Id;

public record ClosedAccount(Id accountId) implements Account {}
