package io.memoria.reactive.eventsourcing.testsuite.banking.domain.state;

import io.memoria.atom.core.id.Id;

public record ClosedAccount(Id accountId) implements Account {}
