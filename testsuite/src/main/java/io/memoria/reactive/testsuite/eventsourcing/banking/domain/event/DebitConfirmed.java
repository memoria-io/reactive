package io.memoria.reactive.testsuite.eventsourcing.banking.domain.event;

import io.memoria.atom.core.id.Id;
import io.memoria.reactive.eventsourcing.CommandId;
import io.memoria.reactive.eventsourcing.EventId;
import io.memoria.reactive.eventsourcing.StateId;
import io.memoria.reactive.testsuite.eventsourcing.banking.domain.command.ConfirmDebit;

public record DebitConfirmed(EventId eventId, CommandId commandId, StateId debitedAcc, long timestamp) implements AccountEvent {
  @Override
  public StateId accountId() {
    return debitedAcc;
  }

  public static DebitConfirmed from(EventId eventId, long timestamp, ConfirmDebit cmd) {
    return new DebitConfirmed(eventId, cmd.commandId(), cmd.debitedAcc(), timestamp);
  }
}
