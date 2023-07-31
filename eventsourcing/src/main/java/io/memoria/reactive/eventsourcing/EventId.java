package io.memoria.reactive.eventsourcing;

import io.memoria.atom.core.id.Id;

import java.util.UUID;

public record EventId(Id id) implements Comparable<EventId> {

  public static EventId of() {
    return new EventId(Id.of());
  }

  public static EventId of(UUID id) {
    return new EventId(Id.of(id));
  }

  public static EventId of(long i) {
    return new EventId(Id.of(i));
  }

  public static EventId of(String value) {
    return new EventId(Id.of(value));
  }

  @Override
  public int compareTo(EventId o) {
    return o.id.compareTo(id);
  }
}
