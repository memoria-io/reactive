package io.memoria.reactive.eventsourcing;

import io.memoria.reactive.eventsourcing.rule.Decider;
import io.memoria.reactive.eventsourcing.rule.Evolver;
import io.memoria.reactive.eventsourcing.rule.Saga;
import io.vavr.control.Option;

public record Domain<S extends State, C extends Command, E extends Event>(Class<S> sClass,
                                                                          Class<C> cClass,
                                                                          Class<E> eClass,
                                                                          Decider<S, C, E> decider,
                                                                          Evolver<S, E> evolver,
                                                                          Saga<E, C> saga) {
  public Domain(Class<S> sClass, Class<C> cClass, Class<E> eClass, Decider<S, C, E> decider, Evolver<S, E> evolver) {
    this(sClass, cClass, eClass, decider, evolver, e -> Option.none());
  }

  public String toShortString() {
    return "Domain(%s,%s,%s)".formatted(sClass.getSimpleName(), cClass.getSimpleName(), eClass.getSimpleName());
  }
}
