package io.memoria.reactive.eventsourcing;

import java.io.Serializable;

public interface Command extends Serializable {
  CommandMeta meta();
}
