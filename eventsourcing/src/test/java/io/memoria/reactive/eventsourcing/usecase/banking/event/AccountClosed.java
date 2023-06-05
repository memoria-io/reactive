package io.memoria.reactive.eventsourcing.usecase.banking.event;

import io.memoria.atom.core.id.Id;
import io.memoria.reactive.eventsourcing.usecase.banking.command.CloseAccount;
import io.memoria.reactive.eventsourcing.usecase.banking.state.Account;

public record AccountClosed(Id eventId, Id commandId, Id accountId) implements AccountEvent {
  @Override
  public Id stateId() {
    return accountId;
  }

  public static AccountClosed from(Account account, CloseAccount cmd) {
    return new AccountClosed(Id.of(), cmd.commandId(), cmd.stateId());
  }
}
