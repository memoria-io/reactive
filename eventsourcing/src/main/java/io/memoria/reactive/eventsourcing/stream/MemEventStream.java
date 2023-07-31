package io.memoria.reactive.eventsourcing.stream;

import io.memoria.reactive.eventsourcing.Event;
import io.memoria.reactive.eventsourcing.EventId;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.publisher.Sinks.Many;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

class MemEventStream<E extends Event> implements EventStream<E> {
  private final int historySize;
  private final Map<String, Map<Integer, Many<E>>> streams = new ConcurrentHashMap<>();
  private final Map<String, Map<Integer, EventId>> last = new ConcurrentHashMap<>();

  public MemEventStream(int historySize) {
    this.historySize = historySize;
  }

  @Override
  public Mono<EventId> last(String topic, int partition) {
    return Mono.defer(() -> Mono.justOrEmpty(last.get(topic))).flatMap(tp -> Mono.justOrEmpty(tp.get(partition)));
  }

  @Override
  public Mono<E> pub(String topic, int partition, E e) {
    return Mono.fromCallable(() -> {
      addPartitionSink(topic, partition);
      return this.publishFn(topic, partition, e);
    });
  }

  @Override
  public Flux<E> sub(String topic, int partition) {
    return Mono.fromCallable(() -> addPartitionSink(topic, partition))
               .flatMapMany(__ -> this.streams.get(topic).get(partition).asFlux());
  }

  private int addPartitionSink(String topic, int partition) {
    this.streams.computeIfAbsent(topic, x -> new ConcurrentHashMap<>());
    this.streams.computeIfPresent(topic, (__, partitions) -> {
      var sink = Sinks.many().replay().<E>limit(historySize);
      partitions.computeIfAbsent(partition, x -> sink);
      return partitions;
    });
    this.last.computeIfAbsent(topic, x -> new ConcurrentHashMap<>());
    return partition;
  }

  private E publishFn(String topic, int partition, E e) {
    this.streams.get(topic).get(partition).tryEmitNext(e);
    this.last.get(topic).put(partition, e.eventId());
    return e;
  }
}
