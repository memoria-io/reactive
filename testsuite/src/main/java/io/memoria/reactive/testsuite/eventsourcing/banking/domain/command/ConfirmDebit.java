package io.memoria.reactive.testsuite.eventsourcing.banking.domain.command;

import io.memoria.atom.eventsourcing.CommandId;
import io.memoria.atom.eventsourcing.StateId;

public record ConfirmDebit(CommandId commandId, StateId debitedAcc, long timestamp) implements AccountCommand {
  @Override
  public StateId accountId() {
    return debitedAcc;
  }
}
