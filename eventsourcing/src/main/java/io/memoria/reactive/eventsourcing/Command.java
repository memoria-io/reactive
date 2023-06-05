package io.memoria.reactive.eventsourcing;

import io.memoria.atom.core.id.Id;

import java.io.Serializable;

public interface Command extends Shardable, Serializable {
  Id commandId();

  long timestamp();
}
