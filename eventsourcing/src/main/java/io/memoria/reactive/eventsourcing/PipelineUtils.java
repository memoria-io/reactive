package io.memoria.reactive.eventsourcing;

import io.memoria.atom.eventsourcing.Event;
import io.memoria.atom.eventsourcing.State;
import io.memoria.atom.eventsourcing.StateId;
import io.memoria.atom.eventsourcing.rule.Evolver;
import io.vavr.collection.HashMap;
import io.vavr.collection.Map;
import io.vavr.control.Option;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

public class PipelineUtils {
  /**
   * Use this method only when the events flux is known to terminate at certain point
   */
  public static <S extends State, E extends Event> Mono<Map<StateId, S>> reduce(Evolver<S, E> evolver, Flux<E> events) {
    java.util.Map<StateId, S> initial = new ConcurrentHashMap<>();
    var result = events.reduce(initial, (map, event) -> {
      map.computeIfPresent(event.stateId(), (k, st) -> evolver.apply(st, event));
      map.computeIfAbsent(event.stateId(), k -> evolver.apply(event));
      return map;
    });
    return result.map(HashMap::ofAll);
  }

  /**
   * Use this method only when the events flux is known to terminate at certain point
   */
  public static <S extends State, E extends Event> Mono<S> reduce(Evolver<S, E> evolver,
                                                                  StateId stateId,
                                                                  Flux<E> events) {
    return events.filter(e -> e.stateId().equals(stateId))
                 .reduce(Option.none(), (Option<S> o, E e) -> applyOpt(evolver, o, e))
                 .filter(Option::isDefined)
                 .map(Option::get);
  }

  /**
   * Filters the events for certain state(stateId) and reducing them into one state after taking (take) number of
   * events
   */
  public static <S extends State, E extends Event> Mono<S> reduce(Evolver<S, E> evolver,
                                                                  StateId stateId,
                                                                  Flux<E> events,
                                                                  int take) {
    return events.filter(e -> e.stateId().equals(stateId))
                 .take(take)
                 .reduce(Option.none(), (Option<S> o, E e) -> applyOpt(evolver, o, e))
                 .filter(Option::isDefined)
                 .map(Option::get);
  }

  public static <S extends State, E extends Event> Flux<S> allStates(Evolver<S, E> evolver,
                                                                     StateId stateId,
                                                                     Flux<E> events) {
    var atomicReference = new AtomicReference<S>();
    return events.filter(e -> e.stateId().equals(stateId)).map(event -> {
      if (atomicReference.get() == null) {
        S newState = evolver.apply(event);
        atomicReference.compareAndExchange(null, newState);
        return newState;
      } else {
        var prevState = atomicReference.get();
        var newState = evolver.apply(prevState, event);
        atomicReference.compareAndExchange(prevState, newState);
        return newState;
      }
    });
  }

  public static <S extends State, E extends Event> Option<S> applyOpt(Evolver<S, E> evolver,
                                                                      Option<S> optState,
                                                                      E event) {
    return optState.map(s -> evolver.apply(s, event)).orElse(() -> Option.some(evolver.apply(event)));
  }
}
