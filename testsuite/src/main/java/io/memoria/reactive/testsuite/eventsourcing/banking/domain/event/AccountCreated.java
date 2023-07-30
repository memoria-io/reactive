package io.memoria.reactive.testsuite.eventsourcing.banking.domain.event;

import io.memoria.atom.core.id.Id;
import io.memoria.reactive.testsuite.eventsourcing.banking.domain.command.CreateAccount;

public record AccountCreated(Id eventId, Id commandId, Id accountId, long timestamp, String name, long balance)
        implements AccountEvent {
  public static AccountCreated from(Id eventId, long timestamp, CreateAccount cmd) {
    return new AccountCreated(eventId, cmd.commandId(), cmd.accountId(), timestamp, cmd.accountName(), cmd.balance());
  }
}
