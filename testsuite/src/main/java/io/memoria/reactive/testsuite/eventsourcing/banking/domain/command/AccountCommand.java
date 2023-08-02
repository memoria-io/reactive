package io.memoria.reactive.testsuite.eventsourcing.banking.domain.command;

import io.memoria.atom.eventsourcing.Command;
import io.memoria.atom.eventsourcing.StateId;

public sealed interface AccountCommand extends Command
        permits ChangeName, CloseAccount, ConfirmDebit, CreateAccount, Credit, Debit {
  StateId accountId();

  default StateId stateId() {
    return accountId();
  }
}
