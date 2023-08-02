package io.memoria.reactive.testsuite.eventsourcing.banking.domain.command;

import io.memoria.reactive.eventsourcing.Command;
import io.memoria.reactive.eventsourcing.StateId;

public sealed interface AccountCommand extends Command
        permits ChangeName, CloseAccount, ConfirmDebit, CreateAccount, Credit, Debit {
  StateId accountId();

  default StateId stateId() {
    return accountId();
  }
}
