package io.memoria.reactive.testsuite.eventsourcing.banking.domain.command;

import io.memoria.atom.core.id.Id;
import io.memoria.reactive.eventsourcing.Command;

public sealed interface AccountCommand extends Command
        permits ChangeName, CloseAccount, ConfirmDebit, CreateAccount, Credit, Debit {
  Id accountId();

  default Id stateId() {
    return accountId();
  }
}
