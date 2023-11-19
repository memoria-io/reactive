package io.memoria.reactive.eventsourcing;

import java.io.Serializable;

public interface Event extends Serializable {
  EventMeta meta();
}
