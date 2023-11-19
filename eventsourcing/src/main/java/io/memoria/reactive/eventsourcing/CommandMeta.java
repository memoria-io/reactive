package io.memoria.reactive.eventsourcing;

import io.vavr.control.Option;

import java.io.Serializable;

public record CommandMeta(io.memoria.reactive.eventsourcing.CommandId commandId,
                          StateId stateId,
                          long timestamp,
                          Option<EventId> sagaSource) implements Shardable, Serializable {
  public CommandMeta {
    if (sagaSource == null) {
      throw new IllegalArgumentException("sagaSource can't be null");
    }
  }

  public CommandMeta(StateId stateId) {
    this(io.memoria.reactive.eventsourcing.CommandId.of(), stateId);
  }

  public CommandMeta(io.memoria.reactive.eventsourcing.CommandId commandId, StateId stateId) {
    this(commandId, stateId, System.currentTimeMillis());
  }

  public CommandMeta(CommandId commandId, StateId stateId, long timestamp) {
    this(commandId, stateId, timestamp, Option.none());
  }
}

