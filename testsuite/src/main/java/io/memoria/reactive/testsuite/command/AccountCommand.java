package io.memoria.reactive.testsuite.command;

import io.memoria.atom.eventsourcing.Command;
import io.memoria.atom.eventsourcing.StateId;

public sealed interface AccountCommand extends Command
        permits ChangeName, CloseAccount, ConfirmDebit, CreateAccount, Credit, Debit {
  default StateId accountId() {
    return meta().stateId();
  }
}
