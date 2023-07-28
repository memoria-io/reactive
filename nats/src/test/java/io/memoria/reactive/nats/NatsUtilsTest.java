package io.memoria.reactive.nats;

import io.memoria.atom.core.text.SerializableTransformer;
import io.memoria.atom.core.text.TextTransformer;
import io.memoria.reactive.eventsourcing.testsuite.banking.domain.event.AccountEvent;
import io.nats.client.Message;
import io.nats.client.Nats;
import io.nats.client.api.DeliverPolicy;
import org.junit.jupiter.api.Test;

import java.io.IOException;

class NatsUtilsTest {
  private static final TextTransformer transformer = new SerializableTransformer();

  @Test
  void consume() throws IOException, InterruptedException {
    try (var nc = Nats.connect(TestUtils.NATS_URL)) {
      var js = NatsUtils.jetStreamSub(nc.jetStream(), DeliverPolicy.All, "20-events-451", 0);
      NatsUtils.fetch(js, TestUtils.natsConfig())
               .map(Message::getData)
               .map(String::new)
               .map(str -> transformer.deserialize(str, AccountEvent.class).get())
               .subscribe(System.out::println);
    }
  }
}
