package io.memoria.reactive.testsuite.eventsourcing.banking.domain.state;

import io.memoria.atom.eventsourcing.State;
import io.memoria.atom.eventsourcing.StateId;

public sealed interface Account extends State permits OpenAccount, ClosedAccount {
  StateId accountId();

  default StateId stateId() {
    return accountId();
  }
}
