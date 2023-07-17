package io.memoria.reactive.eventsourcing.testsuite.banking.domain.event;

import io.memoria.atom.core.id.Id;
import io.memoria.reactive.eventsourcing.testsuite.banking.domain.command.CloseAccount;

public record AccountClosed(Id eventId, Id commandId, Id accountId, long timestamp) implements AccountEvent {

  public static AccountClosed from(Id eventId, long timestamp, CloseAccount cmd) {
    return new AccountClosed(eventId, cmd.commandId(), cmd.stateId(), timestamp);
  }
}
