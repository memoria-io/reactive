package io.memoria.reactive.eventsourcing.usecase.banking.command;

import io.memoria.atom.core.id.Id;

public record Debit(Id commandId, Id debitedAcc, Id creditedAcc, int amount) implements AccountCommand {
  @Override
  public Id accountId() {
    return debitedAcc;
  }

  public static Debit of(Id debitedAcc, Id creditedAcc, int amount) {
    return new Debit(Id.of(), debitedAcc, creditedAcc, amount);
  }
}
