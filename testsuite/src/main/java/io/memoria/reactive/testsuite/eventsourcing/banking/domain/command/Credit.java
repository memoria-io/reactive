package io.memoria.reactive.testsuite.eventsourcing.banking.domain.command;

import io.memoria.reactive.eventsourcing.CommandId;
import io.memoria.reactive.eventsourcing.StateId;

public record Credit(CommandId commandId, StateId creditedAcc, long timestamp, StateId debitedAcc, long amount)
        implements AccountCommand {
  @Override
  public StateId accountId() {
    return creditedAcc;
  }
}
