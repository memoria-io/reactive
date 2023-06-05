package io.memoria.reactive.core.stream;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.publisher.Sinks.Many;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class MemESMsgStream implements ESMsgStream {
  private final int historySize;
  private final Map<String, Map<Integer, Many<ESMsg>>> topics = new ConcurrentHashMap<>();

  public MemESMsgStream() {
    this(Integer.MAX_VALUE);
  }

  public MemESMsgStream(int historySize) {
    this.historySize = historySize;
  }

  @Override
  public Mono<ESMsg> pub(ESMsg msg) {
    return Mono.fromCallable(() -> addPartitionSink(msg.topic(), msg.partition()))
               .flatMap(k -> Mono.fromCallable(() -> this.publishFn(msg)));
  }

  @Override
  public Flux<ESMsg> sub(String topic, int partition) {
    return Mono.fromCallable(() -> addPartitionSink(topic, partition))
               .flatMapMany(i -> this.topics.get(topic).get(partition).asFlux());
  }

  private int addPartitionSink(String topic, int partition) {
    this.topics.computeIfAbsent(topic, x -> new ConcurrentHashMap<>());
    this.topics.computeIfPresent(topic, (k, v) -> {
      var sink = Sinks.many().replay().<ESMsg>limit(historySize);
      v.computeIfAbsent(partition, x -> sink);
      return v;
    });
    return partition;
  }

  private ESMsg publishFn(ESMsg msg) {
    String topic = msg.topic();
    int partition = msg.partition();
    this.topics.get(topic).get(partition).tryEmitNext(msg);
    return msg;
  }
}
