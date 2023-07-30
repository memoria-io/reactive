package io.memoria.reactive.eventsourcing.testsuite.banking.domain.state;

import io.memoria.atom.core.id.Id;
import io.memoria.reactive.eventsourcing.State;

public sealed interface Account extends State permits OpenAccount, ClosedAccount {
  Id accountId();

  default Id stateId() {
    return accountId();
  }
}
