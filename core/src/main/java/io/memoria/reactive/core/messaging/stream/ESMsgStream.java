package io.memoria.reactive.core.messaging.stream;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface ESMsgStream {

  /**
   * @return known key
   */
  Mono<String> last(String topic, int partition);

  Mono<ESMsg> pub(ESMsg esMsg);

  Flux<ESMsg> sub(String topic, int partition);

  /**
   * @return an in memory ESStream
   */
  static ESMsgStream inMemory() {
    return new MemESMsgStream();
  }

  static ESMsgStream inMemory(int historySize) {
    return new MemESMsgStream(historySize);
  }
}