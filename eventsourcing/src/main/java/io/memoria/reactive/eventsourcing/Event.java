package io.memoria.reactive.eventsourcing;

import io.vavr.control.Option;

import java.io.Serializable;

public interface Event extends Shardable, Serializable {
  default Option<EventId> sagaEventId() {
    return Option.none();
  }

  CommandId commandId();

  EventId eventId();

  long timestamp();
}
