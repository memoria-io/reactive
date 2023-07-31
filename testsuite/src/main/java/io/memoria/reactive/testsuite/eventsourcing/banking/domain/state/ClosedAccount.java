package io.memoria.reactive.testsuite.eventsourcing.banking.domain.state;

import io.memoria.atom.core.id.Id;
import io.memoria.reactive.eventsourcing.StateId;

public record ClosedAccount(StateId accountId) implements Account {}
