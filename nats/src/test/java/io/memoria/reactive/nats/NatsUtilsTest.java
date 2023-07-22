package io.memoria.reactive.nats;

import io.memoria.atom.core.text.SerializableTransformer;
import io.memoria.atom.core.text.TextTransformer;
import io.memoria.reactive.eventsourcing.testsuite.banking.domain.event.AccountEvent;
import io.nats.client.JetStreamApiException;
import io.nats.client.Message;
import io.nats.client.Nats;
import org.junit.jupiter.api.Test;

import java.io.IOException;

class NatsUtilsTest {
  private static final TextTransformer transformer = new SerializableTransformer();

  @Test
  void consume() throws IOException, InterruptedException, JetStreamApiException {
    var con = Nats.connect(Utils.NATS_URL);
    NatsUtils.fetch(con, Utils.createTopicConfig("20-events-451", 0))
             .map(Message::getData)
             .map(String::new)
             .map(str -> transformer.deserialize(str, AccountEvent.class).get())
             .forEach(System.out::println);
  }
}
