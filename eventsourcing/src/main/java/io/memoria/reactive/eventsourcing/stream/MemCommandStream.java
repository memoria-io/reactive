package io.memoria.reactive.eventsourcing.stream;

import io.memoria.reactive.eventsourcing.Command;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.publisher.Sinks.Many;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

class MemCommandStream<C extends Command> implements CommandStream<C> {
  private final int historySize;
  private final Map<String, Map<Integer, Many<C>>> streams = new ConcurrentHashMap<>();

  public MemCommandStream(int historySize) {
    this.historySize = historySize;
  }

  @Override
  public Mono<C> pub(String topic, int partition, C c) {
    return Mono.fromCallable(() -> addPartitionSink(topic, partition)).map(__ -> this.publishFn(topic, partition, c));
  }

  @Override
  public Flux<C> sub(String topic, int partition) {
    return Mono.fromCallable(() -> addPartitionSink(topic, partition))
               .flatMapMany(__ -> this.streams.get(topic).get(partition).asFlux());
  }

  private int addPartitionSink(String topic, int partition) {
    this.streams.computeIfAbsent(topic, x -> new ConcurrentHashMap<>());
    this.streams.computeIfPresent(topic, (__, partitions) -> {
      var sink = Sinks.many().replay().<C>limit(historySize);
      partitions.computeIfAbsent(partition, x -> sink);
      return partitions;
    });
    return partition;
  }

  private C publishFn(String topic, int partition, C c) {
    this.streams.get(topic).get(partition).tryEmitNext(c);
    return c;
  }
}
