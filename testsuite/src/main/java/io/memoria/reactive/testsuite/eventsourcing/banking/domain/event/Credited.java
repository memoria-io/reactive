package io.memoria.reactive.testsuite.eventsourcing.banking.domain.event;

import io.memoria.reactive.eventsourcing.CommandId;
import io.memoria.reactive.eventsourcing.EventId;
import io.memoria.reactive.eventsourcing.StateId;
import io.memoria.reactive.testsuite.eventsourcing.banking.domain.command.Credit;

public record Credited(EventId eventId,
                       CommandId commandId,
                       StateId creditedAcc,
                       long timestamp,
                       StateId debitedAcc,
                       long amount) implements AccountEvent {
  @Override
  public StateId accountId() {
    return creditedAcc;
  }

  public static Credited from(EventId eventId, long timestamp, Credit cmd) {
    return new Credited(eventId, cmd.commandId(), cmd.creditedAcc(), timestamp, cmd.debitedAcc(), cmd.amount());
  }
}
