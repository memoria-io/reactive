package io.memoria.reactive.eventsourcing;

import io.memoria.atom.core.id.Id;

import java.util.UUID;

public record CommandId(Id id) implements Comparable<CommandId> {

  public static CommandId of() {
    return new CommandId(Id.of());
  }

  public static CommandId of(UUID id) {
    return new CommandId(Id.of(id));
  }

  public static CommandId of(long i) {
    return new CommandId(Id.of(i));
  }

  public static CommandId of(String value) {
    return new CommandId(Id.of(value));
  }

  @Override
  public int compareTo(CommandId o) {
    return o.id.compareTo(id);
  }
}
