package io.memoria.reactive.nats;

import io.memoria.reactive.core.messaging.stream.ESMsg;
import io.memoria.reactive.nats.messaging.stream.NatsESMsgStream;
import io.nats.client.impl.Headers;
import io.nats.client.impl.NatsMessage;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class NatsUtilsTest {

  @Test
  void toMessage() {
    var message = NatsESMsgStream.toMessage(new ESMsg("topic", 0, 1000 + "", "hello world"));
    Assertions.assertEquals("1000", message.getHeaders().getFirst(NatsUtils.ID_HEADER));
    Assertions.assertEquals("topic_0.subject", message.getSubject());
  }

  @Test
  void toMsg() {
    var h = new Headers();
    h.add(NatsUtils.ID_HEADER, "1000");
    var message = NatsMessage.builder().data("hello world").subject("topic_0.subject").headers(h).build();
    var msg = NatsESMsgStream.toMsg(message);
    Assertions.assertEquals("topic", msg.topic());
    Assertions.assertEquals(0, msg.partition());
  }
}
