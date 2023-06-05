package io.memoria.reactive.eventsourcing.rule;

import io.memoria.atom.core.id.Id;
import io.memoria.reactive.eventsourcing.Event;
import io.memoria.reactive.eventsourcing.State;
import io.vavr.Function2;
import io.vavr.collection.HashMap;
import io.vavr.collection.Map;
import io.vavr.control.Option;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

public interface Evolver<S extends State, E extends Event> extends Function2<S, E, S> {
  S apply(E e);

  /**
   * Use this method only when the events flux is known to terminate at certain point
   */
  default Mono<Map<Id, S>> reduce(Flux<E> events) {
    java.util.Map<Id, S> initial = new ConcurrentHashMap<>();
    var result = events.reduce(initial, (map, event) -> {
      map.computeIfPresent(event.stateId(), (k, st) -> apply(st, event));
      map.computeIfAbsent(event.stateId(), k -> apply(event));
      return map;
    });
    return result.map(HashMap::ofAll);
  }

  /**
   * Use this method only when the events flux is known to terminate at certain point
   */
  default Mono<S> reduce(Id stateId, Flux<E> events) {
    return events.filter(e -> e.stateId().equals(stateId))
                 .reduce(Option.none(), this::applyOpt)
                 .filter(Option::isDefined)
                 .map(Option::get);
  }

  /**
   * Filters the events for certain state(stateId) and reducing them into one state after taking (take) number of
   * events
   */
  default Mono<S> reduce(Id stateId, Flux<E> events, int take) {
    return events.filter(e -> e.stateId().equals(stateId))
                 .take(take)
                 .reduce(Option.none(), this::applyOpt)
                 .filter(Option::isDefined)
                 .map(Option::get);
  }

  default Flux<S> allStates(Id stateId, Flux<E> events) {
    var atomicReference = new AtomicReference<S>();
    return events.filter(e -> e.stateId().equals(stateId)).map(event -> {
      if (atomicReference.get() == null) {
        S newState = apply(event);
        atomicReference.compareAndExchange(null, newState);
        return newState;
      } else {
        var prevState = atomicReference.get();
        var newState = apply(prevState, event);
        atomicReference.compareAndExchange(prevState, newState);
        return newState;
      }
    });
  }

  private Option<S> applyOpt(Option<S> optState, E event) {
    return optState.map(s -> this.apply(s, event)).orElse(() -> Option.some(apply(event)));
  }
}
