package io.memoria.reactive.testsuite.eventsourcing.banking.domain.event;

import io.memoria.atom.core.id.Id;
import io.memoria.reactive.eventsourcing.CommandId;
import io.memoria.reactive.eventsourcing.EventId;
import io.memoria.reactive.eventsourcing.StateId;
import io.memoria.reactive.testsuite.eventsourcing.banking.domain.command.CloseAccount;

public record ClosureRejected(EventId eventId, CommandId commandId, StateId accountId, long timestamp) implements AccountEvent {
  public static ClosureRejected from(EventId eventId, long timestamp, CloseAccount cmd) {
    return new ClosureRejected(eventId, cmd.commandId(), cmd.stateId(), timestamp);
  }
}
