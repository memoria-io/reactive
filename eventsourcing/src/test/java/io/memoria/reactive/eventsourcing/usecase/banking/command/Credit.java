package io.memoria.reactive.eventsourcing.usecase.banking.command;

import io.memoria.atom.core.id.Id;

public record Credit(Id commandId, Id creditedAcc, Id debitedAcc, int amount) implements AccountCommand {
  @Override
  public Id accountId() {
    return creditedAcc;
  }

  public static Credit of(Id creditedAcc, Id debitedAcc, int amount) {
    return new Credit(Id.of(), creditedAcc, debitedAcc, amount);
  }
}
