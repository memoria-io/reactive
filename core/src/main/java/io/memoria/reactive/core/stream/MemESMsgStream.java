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
  private final Map<String, Map<Integer, ESMsg>> lastMsg = new ConcurrentHashMap<>();

  public MemESMsgStream() {
    this(Integer.MAX_VALUE);
  }

  public MemESMsgStream(int historySize) {
    this.historySize = historySize;
  }

  @Override
  public Mono<String> lastKey(String topic, int partition) {
    return Mono.defer(() -> Mono.justOrEmpty(lastMsg.get(topic)))
               .flatMap(tp -> Mono.justOrEmpty(tp.get(partition)))
               .map(ESMsg::key);
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
    this.topics.computeIfPresent(topic, (__, partitions) -> {
      var sink = Sinks.many().replay().<ESMsg>limit(historySize);
      partitions.computeIfAbsent(partition, x -> sink);
      return partitions;
    });
    this.lastMsg.computeIfAbsent(topic, x -> new ConcurrentHashMap<>());
    return partition;
  }

  private ESMsg publishFn(ESMsg msg) {
    String topic = msg.topic();
    int partition = msg.partition();
    this.topics.get(topic).get(partition).tryEmitNext(msg);
    this.lastMsg.get(topic).put(partition, msg);
    return msg;
  }
}
