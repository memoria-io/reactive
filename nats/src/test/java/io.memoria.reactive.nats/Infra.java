package io.memoria.reactive.nats;

import io.memoria.atom.core.text.SerializableTransformer;
import io.memoria.reactive.eventsourcing.stream.MsgStream;
import io.nats.client.Connection;
import io.nats.client.JetStreamApiException;

import java.io.IOException;

import static io.memoria.reactive.nats.NatsUtils.defaultStreamConfig;

public class Infra {
  private final Connection nc;
  private final SerializableTransformer transformer;
  public final MsgStream natsStream;

  public Infra() {
    try {
      // config
      this.nc = NatsUtils.createConnection("nats://localhost:4222");
      this.transformer = new SerializableTransformer();
      // streams
      this.natsStream = new NatsMsgStream(nc);
    } catch (IOException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  public void createNatsTopics(String topic) {
    try {
      NatsUtils.createOrUpdateStream(nc.jetStreamManagement(), defaultStreamConfig(topic, 1).build());
    } catch (IOException | JetStreamApiException e) {
      throw new RuntimeException(e);
    }
  }
}
