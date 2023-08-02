package io.memoria.reactive.testsuite.eventsourcing.banking.domain.event;

import io.memoria.atom.eventsourcing.CommandId;
import io.memoria.atom.eventsourcing.EventId;
import io.memoria.atom.eventsourcing.StateId;
import io.memoria.reactive.testsuite.eventsourcing.banking.domain.command.ChangeName;

public record NameChanged(EventId eventId, CommandId commandId, StateId accountId, long timestamp, String newName)
        implements AccountEvent {
  public static NameChanged from(EventId eventId, long timestamp, ChangeName cmd) {
    return new NameChanged(eventId, cmd.commandId(), cmd.stateId(), timestamp, cmd.name());
  }
}
