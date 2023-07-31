package io.memoria.reactive.eventsourcing;

import io.vavr.control.Option;

import java.io.Serializable;

public interface Command extends Shardable, Serializable {
  Option<EventId> sagaEventId();

  CommandId commandId();

  long timestamp();
}
