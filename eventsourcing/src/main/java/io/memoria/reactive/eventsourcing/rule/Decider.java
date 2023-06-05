package io.memoria.reactive.eventsourcing.rule;

import io.memoria.reactive.eventsourcing.Command;
import io.memoria.reactive.eventsourcing.Event;
import io.memoria.reactive.eventsourcing.State;
import io.vavr.Function2;
import io.vavr.control.Try;

public interface Decider<S extends State, C extends Command, E extends Event> extends Function2<S, C, Try<E>> {
  Try<E> apply(C c);
}
