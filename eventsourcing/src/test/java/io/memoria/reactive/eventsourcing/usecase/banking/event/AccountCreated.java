package io.memoria.reactive.eventsourcing.usecase.banking.event;

import io.memoria.atom.core.id.Id;
import io.memoria.reactive.eventsourcing.usecase.banking.command.CreateAccount;

public record AccountCreated(Id eventId, Id commandId, Id accountId, String name, int balance) implements AccountEvent {
  @Override
  public Id stateId() {
    return accountId;
  }

  public static AccountCreated from(CreateAccount cmd) {
    return new AccountCreated(Id.of(), cmd.commandId(), cmd.accountId(), cmd.accountName(), cmd.balance());
  }
}
