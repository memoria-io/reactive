package io.memoria.reactive.eventsourcing.stream;

import io.memoria.reactive.eventsourcing.CommandId;
import io.memoria.reactive.eventsourcing.Event;
import io.memoria.reactive.eventsourcing.EventId;
import io.memoria.reactive.eventsourcing.StateId;
import io.vavr.control.Option;

record SomeEvent(EventId eventId, StateId stateId, CommandId commandId, Option<EventId> sagaEventId) implements Event {
  public SomeEvent(EventId eventId, StateId stateId, CommandId commandId) {
    this(eventId, stateId, commandId, Option.none());
  }

  @Override
  public long timestamp() {
    return 0;
  }
}
