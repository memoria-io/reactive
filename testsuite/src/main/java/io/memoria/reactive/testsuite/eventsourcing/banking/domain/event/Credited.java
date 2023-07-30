package io.memoria.reactive.testsuite.eventsourcing.banking.domain.event;

import io.memoria.atom.core.id.Id;
import io.memoria.reactive.testsuite.eventsourcing.banking.domain.command.Credit;

public record Credited(Id eventId, Id commandId, Id creditedAcc, long timestamp, Id debitedAcc, long amount)
        implements AccountEvent {
  @Override
  public Id accountId() {
    return creditedAcc;
  }

  public static Credited from(Id eventId, long timestamp, Credit cmd) {
    return new Credited(eventId, cmd.commandId(), cmd.creditedAcc(), timestamp, cmd.debitedAcc(), cmd.amount());
  }
}
