package io.memoria.reactive.eventsourcing.usecase.banking.event;

import io.memoria.atom.core.id.Id;
import io.memoria.reactive.eventsourcing.usecase.banking.command.Debit;
import io.memoria.reactive.eventsourcing.usecase.banking.state.Account;

public record Debited(Id eventId, Id commandId, Id debitedAcc, Id creditedAcc, int amount) implements AccountEvent {
  @Override
  public Id stateId() {
    return debitedAcc;
  }

  public static Debited from(Account account, Debit cmd) {
    return new Debited(Id.of(), cmd.commandId(), cmd.debitedAcc(), cmd.creditedAcc(), cmd.amount());
  }
}
