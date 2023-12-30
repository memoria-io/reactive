//package io.memoria.reactive.testsuite;
//
//import io.memoria.reactive.eventsourcing.stream.Msg;
//import io.memoria.reactive.eventsourcing.stream.MsgStream;
//import io.memoria.reactive.nats.NatsUtils;
//import io.nats.client.JetStreamApiException;
//import org.junit.jupiter.api.BeforeAll;
//import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
//import org.junit.jupiter.api.Named;
//import org.junit.jupiter.api.Order;
//import org.junit.jupiter.api.TestMethodOrder;
//import org.junit.jupiter.params.ParameterizedTest;
//import org.junit.jupiter.params.provider.Arguments;
//import org.junit.jupiter.params.provider.MethodSource;
//import reactor.core.publisher.Flux;
//import reactor.test.StepVerifier;
//
//import java.io.IOException;
//import java.util.stream.Stream;
//
//
//@TestMethodOrder(OrderAnnotation.class)
//class MsgStreamScenarioIT {
//  private static final String topic = Infra.randomTopicName(MsgStreamScenarioIT.class.getSimpleName());
//  private static final int partition = 0;
//  private static final int totalPartitions = 1;
//  private static final int msgCount = 10_000;
//  private static final MsgStream inMemoryStream = msgStream(MEMORY).get();
//  private static final MsgStream kafkaStream = msgStream(KAFKA).get();
//  private static final MsgStream natsStream = msgStream(NATS).get();
//
//  @BeforeAll
//  static void beforeAll() throws JetStreamApiException, IOException, InterruptedException {
//    NatsUtils.createOrUpdateTopic(NATS_CONFIG, topic, totalPartitions);
//  }
//
//  @ParameterizedTest(name = "Using {0} adapter", autoCloseArguments = false)
//  @MethodSource("dataSource")
//  @Order(0)
//  void publish(MsgStream msgStream, int msgCount) {
//    var now = System.currentTimeMillis();
//    var publish = Flux.range(0, msgCount)
//                      .map(i -> new Msg(String.valueOf(i), "hello world"))
//                      .concatMap(msg -> msgStream.pub(topic, partition, msg));
//    StepVerifier.create(publish).expectNextCount(msgCount).verifyComplete();
//    Utils.printRates("publish", now, msgCount);
//  }
//
//  @ParameterizedTest(name = "Using {0} adapter", autoCloseArguments = false)
//  @MethodSource("dataSource")
//  @Order(1)
//  void subscribe(String name, MsgStream msgStream, int msgCount) {
//    var now = System.currentTimeMillis();
//    StepVerifier.create(msgStream.sub(topic, partition)).expectNextCount(msgCount).expectTimeout(TIMEOUT).verify();
//    Utils.printRates("subscribe", now, msgCount);
//  }
//
//  @ParameterizedTest(name = "Using {0} adapter", autoCloseArguments = false)
//  @MethodSource("dataSource")
//  @Order(2)
//  void last(String name, MsgStream msgStream, int msgCount) {
//    var now = System.currentTimeMillis();
//    StepVerifier.create(msgStream.last(topic, partition).map(Msg::key))
//                .expectNext(String.valueOf(msgCount - 1))
//                .verifyComplete();
//    Utils.printRates("subscribe", now, msgCount);
//  }
//
//  private static Stream<Arguments> dataSource() {
//    var arg1 = Arguments.of(Named.of("MEMORY", inMemoryStream, msgCount));
//    var arg2 = Arguments.of(Named.of("KAFKA", kafkaStream, msgCount));
//    var arg3 = Arguments.of(Named.of("NATS", natsStream, msgCount));
//    return Stream.of(arg1, arg2, arg3);
//  }
//}
