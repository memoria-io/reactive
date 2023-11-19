package io.memoria.reactive.eventsourcing;

import io.memoria.atom.core.id.Id;

import java.io.Serializable;
import java.util.UUID;

public record StateId(Id id) implements Comparable<StateId>, Serializable {
  public String value() {
    return id().value();
  }

  public static StateId of() {
    return new StateId(Id.of());
  }

  public static StateId of(Id id) {
    return new StateId(id);
  }

  public static StateId of(UUID id) {
    return new StateId(Id.of(id));
  }

  public static StateId of(long i) {
    return new StateId(Id.of(i));
  }

  public static StateId of(String value) {
    return new StateId(Id.of(value));
  }

  @Override
  public int compareTo(StateId o) {
    return o.id.compareTo(id);
  }
}
