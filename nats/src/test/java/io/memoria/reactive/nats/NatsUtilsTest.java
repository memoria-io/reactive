package io.memoria.reactive.nats;

import io.memoria.reactive.core.stream.Msg;
import io.nats.client.JetStreamApiException;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

class NatsUtilsTest {
  @Test
  void toMessage() {
    var message = NatsUtils.natsMessage("topic", 0, new Msg("0", "hello world"));
    org.junit.jupiter.api.Assertions.assertEquals("topic_0.subject", message.getSubject());
  }

  @Test
  void createTopic() throws JetStreamApiException, IOException, InterruptedException {
    // When
    var infoList = NatsUtils.createOrUpdateTopic(Infra.NATS_CONFIG, "some_topic", 5);
    var topics = infoList.map(info -> info.getConfiguration().getName()).toJavaList();
    // Then
    var expectedTopicNames = List.of("some_topic_0", "some_topic_1", "some_topic_2", "some_topic_3", "some_topic_4");
    Assertions.assertThat(topics).asList().hasSameElementsAs(expectedTopicNames);
  }
}
