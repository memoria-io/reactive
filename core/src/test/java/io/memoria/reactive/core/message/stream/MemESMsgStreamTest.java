package io.memoria.reactive.core.message.stream;

import org.assertj.core.api.Assertions;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

class MemESMsgStreamTest {
  private static final Duration timeout = Duration.ofSeconds(5);
  private static final int ELEMENTS_SIZE = 1000;
  private static final String topic = "some_topic";

  private final ESMsgStream stream = new MemESMsgStream();

  @Test
  void publishAndSubscribe() {
    // Given
    var lastMessage = createMessage(ELEMENTS_SIZE - 1);

    // When
    StepVerifier.create(createMessages().flatMap(msg -> stream.pub(topic, 0, msg)))
                .expectNextCount(ELEMENTS_SIZE)
                .verifyComplete();

    // Then
    var latch = new AtomicInteger();
    stream.sub(topic, 0).take(ELEMENTS_SIZE).index().doOnNext(tup -> {
      Assertions.assertThat(tup.getT2().key()).isEqualTo(String.valueOf(tup.getT1().intValue()));
      latch.incrementAndGet();
    }).subscribe();
    Awaitility.await().atMost(timeout).until(() -> latch.get() == ELEMENTS_SIZE);

    // And
    StepVerifier.create(stream.last(topic, 0)).expectNext(lastMessage.key()).verifyComplete();
  }

  private static Flux<ESMsg> createMessages() {
    return Flux.range(0, ELEMENTS_SIZE).map(MemESMsgStreamTest::createMessage);
  }

  private static ESMsg createMessage(int i) {
    return new ESMsg(String.valueOf(i), "hello");
  }
}
