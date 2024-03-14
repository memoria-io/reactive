package io.memoria.reactive.eventsourcing.pipeline;

import io.memoria.atom.core.text.SerializableTransformer;
import io.memoria.atom.eventsourcing.Domain;
import io.memoria.reactive.eventsourcing.stream.MsgStream;
import io.vavr.Tuple;
import io.vavr.Tuple2;
import io.vavr.collection.List;

import java.time.Duration;

public class Infra {
  private final SerializableTransformer transformer;
  public final MsgStream inMemoryStream;

  public Infra() {
    this.transformer = new SerializableTransformer();
    // streams
    this.inMemoryStream = MsgStream.inMemory();
  }

  public List<Tuple2<CommandRoute, EventRoute>> getRoutes(int x) {
    var cTopic = "commands" + System.currentTimeMillis();
    var eTopic = "events" + System.currentTimeMillis();
    return List.range(0, x).map(i -> Tuple.of(new CommandRoute(cTopic, i, x), new EventRoute(eTopic, i, x)));
  }

  public PartitionPipeline inMemoryPipeline(Domain domain, CommandRoute commandRoute, EventRoute eventRoute, Duration timeout) {
    return new PartitionPipeline(domain, commandRoute, eventRoute, inMemoryStream, timeout, transformer);
  }
}
