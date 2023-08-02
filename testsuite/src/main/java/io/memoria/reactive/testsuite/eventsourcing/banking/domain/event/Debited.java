package io.memoria.reactive.testsuite.eventsourcing.banking.domain.event;

import io.memoria.atom.eventsourcing.CommandId;
import io.memoria.atom.eventsourcing.EventId;
import io.memoria.atom.eventsourcing.StateId;
import io.memoria.reactive.testsuite.eventsourcing.banking.domain.command.Debit;

public record Debited(EventId eventId,
                      CommandId commandId,
                      StateId debitedAcc,
                      long timestamp,
                      StateId creditedAcc,
                      long amount) implements AccountEvent {
  @Override
  public StateId accountId() {
    return debitedAcc;
  }

  public static Debited from(EventId eventId, long timestamp, Debit cmd) {
    return new Debited(eventId, cmd.commandId(), cmd.debitedAcc(), timestamp, cmd.creditedAcc(), cmd.amount());
  }
}
