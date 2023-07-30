package io.memoria.reactive.eventsourcing.testsuite.banking.scenario;

import io.memoria.reactive.core.messaging.stream.ESMsg;
import io.memoria.reactive.core.messaging.stream.ESMsgStream;
import io.vavr.collection.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;

class ESMsgStreamScenario {
  private static final Logger log = LoggerFactory.getLogger(ESMsgStreamScenario.class.getName());
  private final String topic;
  private final int partition;
  private final ESMsgStream repo;

  ESMsgStreamScenario(String topic, int partition, ESMsgStream repo) {
    this.topic = topic;
    this.partition = partition;
    this.repo = repo;
  }

  void publish(int MSG_COUNT) {
    // Given
    var msgs = List.range(0, MSG_COUNT).map(i -> new ESMsg(String.valueOf(i), "hello world"));
    // When
    var pub = Flux.fromIterable(msgs).flatMap(msg -> repo.pub(topic, partition, msg));

  }

  void subscribe(int MSG_COUNT) {
    var sub = repo.sub(topic, partition);
  }
}
