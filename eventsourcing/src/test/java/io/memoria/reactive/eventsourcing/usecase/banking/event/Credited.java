package io.memoria.reactive.eventsourcing.usecase.banking.event;

import io.memoria.atom.core.id.Id;
import io.memoria.reactive.eventsourcing.usecase.banking.command.Credit;
import io.memoria.reactive.eventsourcing.usecase.banking.state.Account;

public record Credited(Id eventId, Id commandId, Id creditedAcc, Id debitedAcc, int amount) implements AccountEvent {
  @Override
  public Id stateId() {
    return creditedAcc;
  }

  public static Credited from(Account account, Credit cmd) {
    return new Credited(Id.of(), cmd.commandId(), cmd.creditedAcc(), cmd.debitedAcc(), cmd.amount());
  }
}
