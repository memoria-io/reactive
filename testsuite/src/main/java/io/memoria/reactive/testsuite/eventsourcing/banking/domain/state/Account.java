package io.memoria.reactive.testsuite.eventsourcing.banking.domain.state;

import io.memoria.atom.core.id.Id;
import io.memoria.reactive.eventsourcing.State;
import io.memoria.reactive.eventsourcing.StateId;

public sealed interface Account extends State permits OpenAccount, ClosedAccount {
  StateId accountId();

  default StateId stateId() {
    return accountId();
  }
}
