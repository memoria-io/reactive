package io.memoria.reactive.testsuite.nats;

import io.memoria.reactive.core.stream.Msg;
import io.memoria.reactive.nats.NatsMsgStream;
import io.memoria.reactive.nats.Utils;
import io.memoria.reactive.testsuite.Infra;
import io.memoria.reactive.testsuite.MsgStreamScenario;
import io.nats.client.JetStreamApiException;
import io.nats.client.api.StreamInfo;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.test.StepVerifier;

import java.io.IOException;

import static io.memoria.reactive.testsuite.Infra.MSG_COUNT;
import static io.memoria.reactive.testsuite.Infra.SCHEDULER;
import static io.memoria.reactive.testsuite.Infra.TIMEOUT;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class NatsMsgStreamTest {
  private static final Logger log = LoggerFactory.getLogger(NatsMsgStreamTest.class.getName());
  private static final MsgStreamScenario scenario;

  static {
    try {
      String topic = Infra.topicName(NatsMsgStreamTest.class);
      Utils.createOrUpdateTopic(NatsInfra.NATS_CONFIG, topic, 1).map(StreamInfo::toString).forEach(log::info);
      var repo = new NatsMsgStream(NatsInfra.NATS_CONFIG, SCHEDULER);
      scenario = new MsgStreamScenario(MSG_COUNT, topic, 0, repo);
    } catch (IOException | InterruptedException | JetStreamApiException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  @Order(0)
  void publish() {
    var now = System.currentTimeMillis();
    StepVerifier.create(scenario.publish()).expectNextCount(MSG_COUNT).verifyComplete();
    Infra.printRates("publish", now);
    //    StepVerifier.create(scenario.last().map(Msg::key)).expectNext(String.valueOf(MSG_COUNT - 1)).verifyComplete();
  }

  @Test
  @Order(1)
  void subscribe() {
    var now = System.currentTimeMillis();
    StepVerifier.create(scenario.subscribe()).expectNextCount(MSG_COUNT).expectTimeout(TIMEOUT).verify();
    Infra.printRates("subscribe", now);
  }

  @Test
  @Order(2)
  void last() {
    StepVerifier.create(scenario.last().map(Msg::key)).expectNext(String.valueOf(MSG_COUNT - 1)).verifyComplete();
  }
}
