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

public class Utils {
  /**
   * Use this method only when the events flux is known to terminate at certain point
   */
  public static Mono<Map<StateId, State>> reduce(Evolver evolver, Flux<Event> events) {
    java.util.Map<StateId, State> initial = new ConcurrentHashMap<>();
    var result = events.reduce(initial, (map, event) -> {
      map.computeIfPresent(event.meta().stateId(), (k, st) -> evolver.apply(st, event));
      map.computeIfAbsent(event.meta().stateId(), k -> evolver.apply(event));
      return map;
    });
    return result.map(HashMap::ofAll);
  }

  /**
   * Use this method only when the events flux is known to terminate at certain point
   */
  public static Mono<State> reduce(Evolver evolver, StateId stateId, Flux<Event> events) {
    return events.filter(e -> e.meta().stateId().equals(stateId))
                 .reduce(Option.none(), (Option<State> o, Event e) -> applyOpt(evolver, o, e))
                 .filter(Option::isDefined)
                 .map(Option::get);
  }

  /**
   * Filters the events for certain state(stateId) and reducing them into one state after taking (take) number of
   * events
   */
  public static Mono<State> reduce(Evolver evolver, StateId stateId, Flux<Event> events, int take) {
    return events.filter(e -> e.meta().stateId().equals(stateId))
                 .take(take)
                 .reduce(Option.none(), (Option<State> o, Event e) -> applyOpt(evolver, o, e))
                 .filter(Option::isDefined)
                 .map(Option::get);
  }

  public static Flux<State> allStates(Evolver evolver, StateId stateId, Flux<Event> events) {
    var atomicReference = new AtomicReference<State>();
    return events.filter(e -> e.meta().stateId().equals(stateId)).map(event -> {
      if (atomicReference.get() == null) {
        State newState = evolver.apply(event);
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

  public static Option<State> applyOpt(Evolver evolver, Option<State> optState, Event event) {
    return optState.map(s -> evolver.apply(s, event)).orElse(() -> Option.some(evolver.apply(event)));
  }

  private Utils() {}
}
