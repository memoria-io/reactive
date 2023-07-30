package io.memoria.reactive.eventsourcing.testsuite.banking.domain.event;

import io.memoria.atom.core.id.Id;
import io.memoria.reactive.eventsourcing.testsuite.banking.domain.command.Debit;

public record Debited(Id eventId, Id commandId, Id debitedAcc, long timestamp, Id creditedAcc, long amount)
        implements AccountEvent {
  @Override
  public Id accountId() {
    return debitedAcc;
  }

  public static Debited from(Id eventId, long timestamp, Debit cmd) {
    return new Debited(eventId, cmd.commandId(), cmd.debitedAcc(), timestamp, cmd.creditedAcc(), cmd.amount());
  }
}
