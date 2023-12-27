package io.memoria.reactive.eventsourcing.stream;

import io.memoria.atom.eventsourcing.Event;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.publisher.Sinks.Many;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

public class MemEventRepo implements EventRepo {
  private final int totalPartitions;
  private final Map<Integer, Many<Event>> events;
  private final Map<Integer, AtomicReference<Event>> lastEvent;

  public MemEventRepo(int totalPartitions) {
    this(totalPartitions, Integer.MAX_VALUE);
  }

  public MemEventRepo(int totalPartitions, int historySize) {
    if (totalPartitions < 1 || historySize < 1) {
      throw new IllegalArgumentException("total partitions or history size can't be less than 1");
    }
    this.totalPartitions = totalPartitions;
    this.events = new ConcurrentHashMap<>();
    this.lastEvent = new ConcurrentHashMap<>();
    for (int i = 0; i < totalPartitions; i++) {
      this.events.put(i, Sinks.many().replay().limit(historySize));
      this.lastEvent.put(i, new AtomicReference<>());
    }
  }

  @Override
  public Mono<Event> publish(Event event) {
    var partition = event.partition(totalPartitions);
    return Mono.fromRunnable(() -> {
      lastEvent.computeIfPresent(partition, (k, v) -> {
        v.set(event);
        return v;
      });
      events.computeIfPresent(partition, (k, v) -> {
        v.tryEmitNext(event).orThrow();
        return v;
      });
    }).thenReturn(event);
  }

  @Override
  public Flux<Event> subscribe(int partition) {
    return events.get(partition).asFlux();
  }

  @Override
  public Mono<Event> last(int partition) {
    return Mono.defer(() -> Mono.justOrEmpty(lastEvent.get(partition))).map(AtomicReference::get);
  }
}
