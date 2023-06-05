package io.memoria.reactive.nats.eventsourcing;

import io.nats.client.api.StorageType;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Duration;

class TopicConfigTest {
  @Test
  void validation() {
    Assertions.assertThatIllegalArgumentException()
              .isThrownBy(() -> new TopicConfig("", 0, StorageType.File, 1, 100, Duration.ofMillis(100), false, false));
    Assertions.assertThatIllegalArgumentException()
              .isThrownBy(() -> new TopicConfig(null,
                                                -1,
                                                StorageType.File,
                                                1,
                                                100,
                                                Duration.ofMillis(100),
                                                false,
                                                false));
    Assertions.assertThatIllegalArgumentException()
              .isThrownBy(() -> new TopicConfig("topic",
                                                -1,
                                                StorageType.File,
                                                1,
                                                100,
                                                Duration.ofMillis(100),
                                                false,
                                                false));
    Assertions.assertThatIllegalArgumentException()
              .isThrownBy(() -> new TopicConfig("topic",
                                                0,
                                                StorageType.File,
                                                0,
                                                100,
                                                Duration.ofMillis(100),
                                                false,
                                                false));
    Assertions.assertThatIllegalArgumentException()
              .isThrownBy(() -> new TopicConfig("topic",
                                                0,
                                                StorageType.File,
                                                6,
                                                100,
                                                Duration.ofMillis(100),
                                                false,
                                                false));
    Assertions.assertThatIllegalArgumentException()
              .isThrownBy(() -> new TopicConfig("topic",
                                                0,
                                                StorageType.File,
                                                1,
                                                -1,
                                                Duration.ofMillis(100),
                                                false,
                                                false));
  }
}
