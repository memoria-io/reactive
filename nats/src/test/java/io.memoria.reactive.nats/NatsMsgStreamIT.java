package io.memoria.reactive.nats;

import io.memoria.reactive.eventsourcing.stream.Msg;
import io.memoria.reactive.eventsourcing.stream.MsgStream;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.stream.Stream;

@TestMethodOrder(OrderAnnotation.class)
class NatsMsgStreamIT {
  private static final Logger log = LoggerFactory.getLogger(NatsMsgStreamIT.class.getName());

  // Infra
  private static final Infra infra = new Infra();

  private static final String topic = "topic" + System.currentTimeMillis();
  private static final int partition = 0;
  private static final int totalPartitions = 1;
  private static final int msgCount = 1000;

  @BeforeAll
  static void beforeAll() {
    infra.createNatsTopics(topic);
  }

  @ParameterizedTest(name = "Using {0} adapter")
  @MethodSource("dataSource")
  @Order(0)
  void publish(MsgStream msgStream) {
    var now = System.currentTimeMillis();
    var publish = Flux.range(0, msgCount)
                      .map(i -> new Msg(topic, partition, String.valueOf(i), "hello world"))
                      .concatMap(msgStream::pub);
    StepVerifier.create(publish).expectNextCount(msgCount).verifyComplete();
    printRates("publish", now, msgCount);
  }

  @ParameterizedTest(name = "Using {0} adapter")
  @MethodSource("dataSource")
  @Order(1)
  void subscribe(MsgStream msgStream) {
    var now = System.currentTimeMillis();
    StepVerifier.create(msgStream.sub(topic, partition))
                .expectNextCount(msgCount)
                .expectTimeout(Duration.ofMillis(1000))
                .verify();
    printRates("subscribe", now, msgCount);
  }

  @ParameterizedTest(name = "Using {0} adapter")
  @MethodSource("dataSource")
  @Order(2)
  void last(MsgStream msgStream) {
    var now = System.currentTimeMillis();
    StepVerifier.create(msgStream.last(topic, partition).map(Msg::key))
                .expectNext(String.valueOf(msgCount - 1))
                .verifyComplete();
    log.info("Fetched last message in %d milliseconds".formatted(System.currentTimeMillis() - now));
  }

  private static void printRates(String methodName, long start, long msgCount) {
    long totalElapsed = System.currentTimeMillis() - start;
    log.info("{}: Finished processing {} events, in {} millis %n", methodName, msgCount, totalElapsed);
    var eventsPerSec = msgCount / (totalElapsed / 1000d);
    log.info("{}: Average {} events per second %n", methodName, (long) eventsPerSec);
  }

  private static Stream<Arguments> dataSource() {
    var arg3 = Arguments.of(Named.of("NATS", infra.natsStream));
    return Stream.of(arg3);
  }
}
