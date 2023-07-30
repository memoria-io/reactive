package io.memoria.reactive.testsuite.eventsourcing.banking.domain.command;

import io.memoria.atom.core.id.Id;

public record Debit(Id commandId, Id debitedAcc, long timestamp, Id creditedAcc, long amount)
        implements AccountCommand {
  @Override
  public Id accountId() {
    return debitedAcc;
  }
}
