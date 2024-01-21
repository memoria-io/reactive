package io.memoria.reactive.nats;

import io.memoria.atom.core.text.SerializableTransformer;
import io.memoria.atom.eventsourcing.Domain;
import io.memoria.reactive.eventsourcing.pipeline.CommandRoute;
import io.memoria.reactive.eventsourcing.pipeline.EventRoute;
import io.memoria.reactive.eventsourcing.pipeline.PartitionPipeline;
import io.memoria.reactive.eventsourcing.stream.MsgStream;
import io.nats.client.Connection;
import io.nats.client.JetStreamApiException;
import io.vavr.Tuple;
import io.vavr.Tuple2;
import io.vavr.collection.List;

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

  public List<Tuple2<CommandRoute, EventRoute>> getRoutes(int x) {
    var cTopic = "commands" + System.currentTimeMillis();
    var eTopic = "events" + System.currentTimeMillis();
    return List.range(0, x).map(i -> Tuple.of(new CommandRoute(cTopic, i, x), new EventRoute(eTopic, i, x)));
  }

  public void createNatsTopics(String topic) {
    try {
      NatsUtils.createOrUpdateStream(nc.jetStreamManagement(), defaultStreamConfig(topic, 1).build());
    } catch (IOException | JetStreamApiException e) {
      throw new RuntimeException(e);
    }
  }

  public PartitionPipeline natsPipeline(Domain domain, CommandRoute commandRoute, EventRoute eventRoute) {
    return new PartitionPipeline(domain, commandRoute, eventRoute, natsStream, transformer);
  }
}
