package io.memoria.reactive.eventsourcing.rule;

import io.memoria.reactive.eventsourcing.Command;
import io.memoria.reactive.eventsourcing.Event;
import io.vavr.Function1;
import io.vavr.control.Option;

@FunctionalInterface
public interface Saga<E extends Event, C extends Command> extends Function1<E, Option<C>> {}
