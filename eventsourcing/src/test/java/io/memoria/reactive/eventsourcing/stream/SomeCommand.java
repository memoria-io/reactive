package io.memoria.reactive.eventsourcing.stream;

import io.memoria.atom.core.id.Id;
import io.memoria.reactive.eventsourcing.Command;

record SomeCommand(Id eventId, Id stateId, Id commandId) implements Command {
  @Override
  public long timestamp() {
    return 0;
  }
}
