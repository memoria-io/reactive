package io.memoria.reactive.eventsourcing.stream;

import io.memoria.atom.core.id.Id;
import io.memoria.reactive.eventsourcing.Event;

public record SomeEvent(Id eventId, Id stateId, Id commandId) implements Event {
  @Override
  public long timestamp() {
    return 0;
  }
}
