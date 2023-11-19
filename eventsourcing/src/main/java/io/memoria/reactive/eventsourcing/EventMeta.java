package io.memoria.reactive.eventsourcing;

import io.vavr.control.Option;

import java.io.Serializable;

public record EventMeta(EventId eventId,
                        long version,
                        StateId stateId,
                        CommandId commandId,
                        long timestamp,
                        Option<EventId> sagaSource) implements Shardable, Versioned, Serializable {
  public EventMeta {
    if (version < 0) {
      throw new IllegalArgumentException("version can't be less than zero");
    }
    if (sagaSource == null) {
      throw new IllegalArgumentException("Saga source can't be null");
    }

  }

  public EventMeta(CommandId commandId, long version, StateId stateId) {
    this(EventId.of(), version, stateId, commandId);
  }

  public EventMeta(EventId id, long version, StateId stateId, CommandId commandId) {
    this(id, version, stateId, commandId, System.currentTimeMillis());
  }

  public EventMeta(EventId id, long version, StateId stateId, CommandId commandId, long timestamp) {
    this(id, version, stateId, commandId, timestamp, Option.none());
  }
}

