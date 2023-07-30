package io.memoria.reactive.nats;

import io.memoria.atom.core.text.SerializableTransformer;
import io.memoria.atom.core.text.TextTransformer;
import io.nats.client.JetStreamApiException;
import io.nats.client.Nats;
import io.nats.client.api.StreamInfo;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static io.memoria.reactive.nats.TestUtils.natsConfig;

class NatsUtilsTest {
  private static final Logger log = LoggerFactory.getLogger(NatsUtilsTest.class.getName());
  private static final TextTransformer transformer = new SerializableTransformer();

  @Test
  void consume() throws IOException, InterruptedException {
    try (var nc = Nats.connect(TestUtils.NATS_URL)) {

      String topic = "20-events-451";
      int numOfPartitions = 1;
      NatsUtils.createOrUpdateTopic(natsConfig, topic, numOfPartitions).map(StreamInfo::toString).forEach(log::info);
      //      var js = NatsUtils.jetStreamSub(nc.jetStream(), DeliverPolicy.All, topic, partition);
      //      NatsUtils.fetch(js, natsConfig)
      //               .map(Message::getData)
      //               .map(String::new)
      //               .map(str -> transformer.deserialize(str, AccountEvent.class).get())
      //               .subscribe(System.out::println);
    } catch (JetStreamApiException e) {
      throw new RuntimeException(e);
    }
  }
}
