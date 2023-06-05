package io.memoria.reactive.nats.eventsourcing;

import io.memoria.reactive.core.stream.ESMsgStream;
import io.nats.client.Connection;

public interface NatsESMsgStream extends ESMsgStream {

  static NatsESMsgStream create(Connection nc, NatsConfig natsConfig) {
    return new DefaultNatsESMsgStream(nc, natsConfig);
  }
}
