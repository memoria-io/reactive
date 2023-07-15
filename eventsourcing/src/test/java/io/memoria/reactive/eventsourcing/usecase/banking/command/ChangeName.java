package io.memoria.reactive.eventsourcing.usecase.banking.command;

import io.memoria.atom.core.id.Id;

public record ChangeName(Id commandId, Id accountId, String name) implements AccountCommand {
  public static AccountCommand of(Id accountId, String name) {
    return new ChangeName(Id.of(), accountId, name);
  }
}
