package io.memoria.reactive.eventsourcing.pipeline;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

class EventRouteTest {

  private static final String VALID_TOPIC = "some_topic";
  private static final int VALID_PARTITION = 0;
  private static final int validTotalPartitions = 1;

  @Test
  void shouldPass() {
    assertThat(new EventRoute(VALID_TOPIC)).isEqualTo(new EventRoute(VALID_TOPIC,
                                                                         VALID_PARTITION,
                                                                         validTotalPartitions));
  }

  @Test
  void topicShouldNotBeNull() {
    assertThatNullPointerException().isThrownBy(() -> new EventRoute(null, VALID_PARTITION, validTotalPartitions));
  }

  @ParameterizedTest
  @MethodSource("args")
  void shouldFailOnInvalidArguments(String topic, int partition, int totalPartitions) {
    assertThatIllegalArgumentException().isThrownBy(() -> new EventRoute(topic, partition, totalPartitions));
  }

  private static Stream<Arguments> args() {
    return Stream.of(Arguments.of("", VALID_PARTITION, validTotalPartitions),
                     Arguments.of(" ", VALID_PARTITION, validTotalPartitions),
                     Arguments.of("with space", VALID_PARTITION, validTotalPartitions),
                     Arguments.of(VALID_TOPIC, -1, validTotalPartitions),
                     Arguments.of(VALID_TOPIC, VALID_PARTITION, 0),
                     Arguments.of(VALID_TOPIC, 2, 1));
  }
}
