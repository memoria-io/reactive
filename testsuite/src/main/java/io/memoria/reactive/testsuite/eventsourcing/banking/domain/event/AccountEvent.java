package io.memoria.reactive.testsuite.eventsourcing.banking.domain.event;

import io.memoria.reactive.eventsourcing.Event;
import io.memoria.reactive.eventsourcing.StateId;

public sealed interface AccountEvent extends Event permits AccountClosed,
                                                           AccountCreated,
                                                           ClosureRejected,
                                                           CreditRejected,
                                                           Credited,
                                                           DebitConfirmed,
                                                           Debited,
                                                           NameChanged {
  StateId accountId();

  default StateId stateId() {
    return accountId();
  }
}
