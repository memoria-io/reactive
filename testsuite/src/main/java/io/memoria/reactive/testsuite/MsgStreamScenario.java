package io.memoria.reactive.testsuite;

import io.memoria.reactive.core.stream.Msg;
import io.memoria.reactive.core.stream.MsgStream;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class MsgStreamScenario {
  private final int msgCount;
  private final String topic;
  private final int partition;
  private final MsgStream stream;

  public MsgStreamScenario(int msgCount, String topic, int partition, MsgStream stream) {
    this.msgCount = msgCount;
    this.topic = topic;
    this.partition = partition;
    this.stream = stream;
  }

  public Flux<Msg> publish() {
    var msgs = Flux.range(0, msgCount).map(i -> new Msg(String.valueOf(i), "hello world"));
    return msgs.concatMap(msg -> stream.pub(topic, partition, msg));
  }

  public Flux<Msg> subscribe() {
    return stream.sub(topic, partition);
  }

  public Mono<Msg> last() {
    return stream.last(topic, partition);
  }
}
