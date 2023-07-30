package io.memoria.reactive.eventsourcing.testsuite.banking.domain.event;

import io.memoria.atom.core.id.Id;
import io.memoria.reactive.eventsourcing.testsuite.banking.domain.command.ChangeName;

public record NameChanged(Id eventId, Id commandId, Id accountId, long timestamp, String newName)
        implements AccountEvent {
  public static NameChanged from(Id eventId, long timestamp, ChangeName cmd) {
    return new NameChanged(eventId, cmd.commandId(), cmd.stateId(), timestamp, cmd.name());
  }
}
