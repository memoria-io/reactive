package io.memoria.reactive.eventsourcing.testsuite.banking.domain.command;

import io.memoria.atom.core.id.Id;

public record ConfirmDebit(Id commandId, Id debitedAcc, long timestamp) implements AccountCommand {
  @Override
  public Id accountId() {
    return debitedAcc;
  }
}
