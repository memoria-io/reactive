package io.memoria.reactive.testsuite.eventsourcing.banking.domain.state;

import io.memoria.atom.core.id.Id;

public record ClosedAccount(Id accountId) implements Account {}
