package io.memoria.reactive.eventsourcing.usecase.banking.event;

import io.memoria.atom.core.id.Id;
import io.memoria.reactive.eventsourcing.usecase.banking.command.Credit;
import io.memoria.reactive.eventsourcing.usecase.banking.state.Account;

public record CreditRejected(Id eventId, Id commandId, Id creditedAcc, Id debitedAcc, int amount)
        implements AccountEvent {
  @Override
  public Id stateId() {
    return creditedAcc;
  }

  public static CreditRejected from(Account acc, Credit cmd) {
    return new CreditRejected(Id.of(), cmd.commandId(), cmd.creditedAcc(), cmd.debitedAcc(), cmd.amount());
  }
}
