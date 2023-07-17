package io.memoria.reactive.eventsourcing.testsuite.banking.domain.command;

import io.memoria.atom.core.id.Id;

public record Credit(Id commandId, Id creditedAcc, long timestamp, Id debitedAcc, long amount)
        implements AccountCommand {
  @Override
  public Id accountId() {
    return creditedAcc;
  }
}
