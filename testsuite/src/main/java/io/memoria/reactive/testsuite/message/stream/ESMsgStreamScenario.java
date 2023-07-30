package io.memoria.reactive.testsuite.message.stream;

import io.memoria.reactive.core.message.stream.ESMsg;
import io.memoria.reactive.core.message.stream.ESMsgStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class ESMsgStreamScenario {
  private static final Logger log = LoggerFactory.getLogger(ESMsgStreamScenario.class.getName());
  private final int msgCount;
  private final String topic;
  private final int partition;
  private final ESMsgStream repo;

  public ESMsgStreamScenario(int msgCount, String topic, int partition, ESMsgStream repo) {
    this.msgCount = msgCount;
    this.topic = topic;
    this.partition = partition;
    this.repo = repo;
  }

  public Flux<ESMsg> publish() {
    var msgs = Flux.range(0, msgCount).map(i -> new ESMsg(String.valueOf(i), "hello world"));
    return msgs.flatMap(msg -> repo.pub(topic, partition, msg));
  }

  public Flux<ESMsg> subscribe() {
    return repo.sub(topic, partition);
  }

  public Mono<String> last() {
    return repo.last(topic, partition);
  }
}
