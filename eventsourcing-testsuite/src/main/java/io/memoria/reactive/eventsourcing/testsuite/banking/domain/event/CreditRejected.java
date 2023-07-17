package io.memoria.reactive.eventsourcing.testsuite.banking.domain.event;

import io.memoria.atom.core.id.Id;
import io.memoria.reactive.eventsourcing.testsuite.banking.domain.command.Credit;

public record CreditRejected(Id eventId, Id commandId, Id creditedAcc, long timestamp, Id debitedAcc, long amount)
        implements AccountEvent {
  @Override
  public Id accountId() {
    return creditedAcc;
  }

  public static CreditRejected from(Id eventId, long timestamp, Credit cmd) {
    return new CreditRejected(eventId, cmd.commandId(), cmd.creditedAcc(), timestamp, cmd.debitedAcc(), cmd.amount());
  }
}
