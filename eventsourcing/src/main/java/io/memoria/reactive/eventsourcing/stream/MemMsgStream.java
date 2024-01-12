package io.memoria.reactive.eventsourcing.stream;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.publisher.Sinks.Many;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

public class MemMsgStream implements MsgStream {
  private final Map<String, Map<Integer, Many<Msg>>> messages;
  private final Map<String, Map<Integer, AtomicReference<Msg>>> last;
  private final int historySize;

  public MemMsgStream() {
    this(new ConcurrentHashMap<>(), new ConcurrentHashMap<>(), Integer.MAX_VALUE);
  }

  public MemMsgStream(Map<String, Map<Integer, Many<Msg>>> messages,
                      Map<String, Map<Integer, AtomicReference<Msg>>> last,
                      int historySize) {
    this.messages = messages;
    this.last = last;
    this.historySize = historySize;
  }

  @Override
  public Mono<Msg> pub(Msg msg) {
    return Mono.fromRunnable(() -> emit(msg)).thenReturn(msg);
  }

  private void emit(Msg msg) {
    messages.computeIfAbsent(msg.topic(), _ -> new ConcurrentHashMap<>());
    last.computeIfAbsent(msg.topic(), _ -> new ConcurrentHashMap<>());
    messages.computeIfPresent(msg.topic(), (_, topicStream) -> {
      topicStream.computeIfAbsent(msg.partition(), _ -> Sinks.many().replay().limit(historySize));
      topicStream.computeIfPresent(msg.partition(), (_, v) -> {
        v.tryEmitNext(msg).orThrow();
        return v;
      });
      return topicStream;
    });
    last.computeIfPresent(msg.topic(), (_, topicStream) -> {
      topicStream.computeIfAbsent(msg.partition(), _ -> new AtomicReference<>(msg));
      topicStream.computeIfPresent(msg.partition(), (_, v) -> {
        v.set(msg);
        return v;
      });
      return topicStream;
    });
  }

  @Override
  public Flux<Msg> sub(String topic, int partition) {
    return Mono.justOrEmpty(messages.get(topic))
               .flatMap(tp -> Mono.justOrEmpty(tp.get(partition)))
               .flatMapMany(Many::asFlux);
  }

  @Override
  public Mono<Msg> last(String topic, int partition) {
    return Mono.justOrEmpty(last.get(topic))
               .flatMap(tp -> Mono.justOrEmpty(tp.get(partition)))
               .flatMap(l -> Mono.justOrEmpty(l.get()));
  }
}
