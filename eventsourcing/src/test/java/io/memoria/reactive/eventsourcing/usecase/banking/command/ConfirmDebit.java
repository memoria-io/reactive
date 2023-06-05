package io.memoria.reactive.eventsourcing.usecase.banking.command;

import io.memoria.atom.core.id.Id;

public record ConfirmDebit(Id commandId, Id debitedAcc) implements AccountCommand {
  @Override
  public Id accountId() {
    return debitedAcc;
  }

  public static ConfirmDebit of(Id debitedAcc) {
    return new ConfirmDebit(Id.of(), debitedAcc);
  }
}
