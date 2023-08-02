package io.memoria.reactive.testsuite.eventsourcing.banking.domain.command;

import io.memoria.atom.eventsourcing.CommandId;
import io.memoria.atom.eventsourcing.StateId;

public record Debit(CommandId commandId, StateId debitedAcc, long timestamp, StateId creditedAcc, long amount)
        implements AccountCommand {
  @Override
  public StateId accountId() {
    return debitedAcc;
  }
}
