package io.memoria.reactive.eventsourcing.usecase.banking.command;

import io.memoria.atom.core.id.Id;

public record CloseAccount(Id commandId, Id accountId) implements AccountCommand {
  public static CloseAccount of(Id accountId) {
    return new CloseAccount(Id.of(), accountId);
  }
}
