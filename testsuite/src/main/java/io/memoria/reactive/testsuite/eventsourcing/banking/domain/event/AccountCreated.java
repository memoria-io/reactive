package io.memoria.reactive.testsuite.eventsourcing.banking.domain.event;

import io.memoria.atom.eventsourcing.CommandId;
import io.memoria.atom.eventsourcing.EventId;
import io.memoria.atom.eventsourcing.StateId;
import io.memoria.reactive.testsuite.eventsourcing.banking.domain.command.CreateAccount;

public record AccountCreated(EventId eventId,
                             CommandId commandId,
                             StateId accountId,
                             long timestamp,
                             String name,
                             long balance) implements AccountEvent {
  public static AccountCreated from(EventId eventId, long timestamp, CreateAccount cmd) {
    return new AccountCreated(eventId, cmd.commandId(), cmd.accountId(), timestamp, cmd.accountName(), cmd.balance());
  }
}
