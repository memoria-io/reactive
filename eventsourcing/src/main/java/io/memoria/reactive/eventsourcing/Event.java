package io.memoria.reactive.eventsourcing;

import io.vavr.control.Option;

import java.io.Serializable;

public interface Event extends Shardable, Serializable {
  Option<EventId> sagaEventId();

  CommandId commandId();

  EventId eventId();

  long timestamp();
}
