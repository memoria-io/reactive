package io.memoria.reactive.nats;

import io.memoria.reactive.core.stream.Msg;
import io.nats.client.api.StorageType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Duration;

class NatsMsgStreamTest {
  public static final String NATS_URL = "nats://localhost:4222";
  public static final NatsConfig NATS_CONFIG = NatsConfig.appendOnly(NATS_URL,
                                                                     StorageType.File,
                                                                     1,
                                                                     1000,
                                                                     Duration.ofMillis(100),
                                                                     Duration.ofMillis(300));

  @Test
  void toMessage() {
    var message = NatsMsgStream.natsMessage("topic", 0, new Msg("0", "hello world"));
    Assertions.assertEquals("topic_0.subject", message.getSubject());
  }
}
