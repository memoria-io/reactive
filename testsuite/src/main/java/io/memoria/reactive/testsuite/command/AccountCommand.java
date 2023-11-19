package io.memoria.reactive.testsuite.command;

import io.memoria.reactive.eventsourcing.Command;
import io.memoria.reactive.eventsourcing.StateId;

public sealed interface AccountCommand extends Command
        permits ChangeName, CloseAccount, ConfirmDebit, CreateAccount, Credit, Debit {
  default StateId accountId() {
    return meta().stateId();
  }
}
