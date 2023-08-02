package io.memoria.reactive.core.msg.stream;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface MsgStream {

  /**
   * @return known key
   */
  Mono<Msg> last(String topic, int partition);

  Mono<Msg> pub(String topic, int partition, Msg msg);

  Flux<Msg> sub(String topic, int partition);

  /**
   * @return an in memory ESStream
   */
  static MsgStream inMemory() {
    return new MemMsgStream();
  }

  static MsgStream inMemory(int historySize) {
    return new MemMsgStream(historySize);
  }
}
