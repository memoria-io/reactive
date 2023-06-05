package io.memoria.reactive.eventsourcing.usecase.banking.command;

import io.memoria.atom.core.id.Id;

public record CreateAccount(Id commandId, Id accountId, String accountName, int balance) implements AccountCommand {
  public static CreateAccount of(Id accountId, String accountName, int balance) {
    return new CreateAccount(Id.of(), accountId, accountName, balance);
  }
}
