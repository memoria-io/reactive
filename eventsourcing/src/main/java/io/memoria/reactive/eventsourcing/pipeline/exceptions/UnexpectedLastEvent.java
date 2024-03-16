package io.memoria.reactive.eventsourcing.pipeline.exceptions;

import io.memoria.atom.eventsourcing.ESException;
import io.memoria.atom.eventsourcing.event.Event;

public class UnexpectedLastEvent extends ESException {

  protected UnexpectedLastEvent(String msg) {
    super(msg);
  }

  public static UnexpectedLastEvent of(Event event) {
    String msg = STR."Invalid initial event: %s[%s] for state".formatted(event.getClass().getSimpleName(),
                                                                         event.meta());
    return new UnexpectedLastEvent(msg);
  }
}
