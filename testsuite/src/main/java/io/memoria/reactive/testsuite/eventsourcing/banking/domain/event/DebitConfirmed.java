package io.memoria.reactive.testsuite.eventsourcing.banking.domain.event;

import io.memoria.atom.eventsourcing.CommandId;
import io.memoria.atom.eventsourcing.EventId;
import io.memoria.atom.eventsourcing.StateId;
import io.memoria.reactive.testsuite.eventsourcing.banking.domain.command.ConfirmDebit;

public record DebitConfirmed(EventId eventId, CommandId commandId, StateId debitedAcc, long timestamp)
        implements AccountEvent {
  @Override
  public StateId accountId() {
    return debitedAcc;
  }

  public static DebitConfirmed from(EventId eventId, long timestamp, ConfirmDebit cmd) {
    return new DebitConfirmed(eventId, cmd.commandId(), cmd.debitedAcc(), timestamp);
  }
}
