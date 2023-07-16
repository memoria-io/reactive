package io.memoria.reactive.eventsourcing.testsuite.banking.domain.event;

import io.memoria.atom.core.id.Id;
import io.memoria.reactive.eventsourcing.testsuite.banking.domain.command.ConfirmDebit;

public record DebitConfirmed(Id eventId, Id commandId, Id debitedAcc, long timestamp) implements AccountEvent {
  @Override
  public Id accountId() {
    return debitedAcc;
  }

  public static DebitConfirmed from(Id eventId, long timestamp, ConfirmDebit cmd) {
    return new DebitConfirmed(eventId, cmd.commandId(), cmd.debitedAcc(), timestamp);
  }
}
