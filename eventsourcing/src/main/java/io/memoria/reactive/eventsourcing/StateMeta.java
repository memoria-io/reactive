package io.memoria.reactive.eventsourcing;

import java.io.Serializable;

public record StateMeta(StateId stateId, long version) implements Shardable, Versioned, Serializable {
  public StateMeta {
    if (version < 0) {
      throw new IllegalArgumentException("Version can't be less than zero!");
    }
  }

  public StateMeta() {
    this(StateId.of());
  }

  public StateMeta(StateId stateId) {
    this(stateId, 0);
  }

  public StateMeta incrementVersion() {
    return new StateMeta(stateId, version + 1);
  }
}
