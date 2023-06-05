package io.memoria.reactive.eventsourcing.usecase.banking.event;

import io.memoria.atom.core.id.Id;
import io.memoria.reactive.eventsourcing.usecase.banking.command.CloseAccount;
import io.memoria.reactive.eventsourcing.usecase.banking.state.Account;

public record ClosureRejected(Id eventId, Id commandId, Id accountId) implements AccountEvent {
  @Override
  public Id stateId() {
    return accountId;
  }

  public static ClosureRejected from(Account account, CloseAccount cmd) {
    return new ClosureRejected(Id.of(), cmd.commandId(), cmd.stateId());
  }
}
