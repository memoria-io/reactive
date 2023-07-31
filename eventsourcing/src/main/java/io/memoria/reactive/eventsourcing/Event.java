package io.memoria.reactive.eventsourcing;

import io.memoria.atom.core.id.Id;
import io.vavr.control.Option;

import java.io.Serializable;

public interface Event extends Shardable, Serializable {
  Option<Id> sagaEventId();

  Id commandId();

  Id eventId();

  long timestamp();
}
