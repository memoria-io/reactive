package io.memoria.reactive.eventsourcing.stream;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks.Many;

import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

public interface MsgStream {
  Mono<Msg> pub(Msg msg);

  Flux<Msg> sub(String topic, int partition);

  Mono<Msg> last(String topic, int partition);

  /**
   * @return subscribe until eventId (key) is matched
   */
  default Flux<Msg> subUntil(String topic, int partition, String key) {
    return sub(topic, partition).takeUntil(msg -> msg.key().equals(key));
  }

  static MsgStream inMemory() {
    return new MemMsgStream();
  }

  static MsgStream inMemory(Map<String, Map<Integer, Many<Msg>>> messages,
                            Map<String, Map<Integer, AtomicReference<Msg>>> last,
                            int historySize) {
    return new MemMsgStream(messages, last, historySize);
  }
}

