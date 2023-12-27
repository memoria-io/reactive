package io.memoria.reactive.testsuite;

import io.memoria.atom.eventsourcing.Domain;
import io.memoria.reactive.eventsourcing.pipeline.PartitionPipeline;
import io.memoria.reactive.eventsourcing.stream.CommandRepo;
import io.memoria.reactive.eventsourcing.stream.EventRepo;
import io.memoria.reactive.kafka.KafkaCommandRepo;
import io.memoria.reactive.kafka.KafkaEventRepo;
import io.memoria.reactive.nats.NatsCommandRepo;
import io.memoria.reactive.nats.NatsEventRepo;
import io.memoria.reactive.nats.NatsUtils;
import io.nats.client.JetStreamApiException;
import io.nats.client.JetStreamManagement;

import java.io.IOException;
import java.time.Duration;

public class Infra {

  private Infra() {}

  public static PartitionPipeline kafkaPipeline(Configs configs, Domain domain) {
    var commandRepo = new KafkaCommandRepo(configs.kafkaProducerConfigs(),
                                           configs.kafkaConsumerConfigs(),
                                           configs.commandsTopic,
                                           configs.totalCommandPartitions,
                                           configs.transformer);
    var eventRepo = new KafkaEventRepo(configs.kafkaProducerConfigs(),
                                       configs.kafkaConsumerConfigs(),
                                       configs.eventsTopic,
                                       configs.totalEventPartitions,
                                       Duration.ofMillis(1000),
                                       configs.transformer);
    return new PartitionPipeline(domain, commandRepo, eventRepo, configs.eventPartition);
  }

  public static PartitionPipeline natsPipeline(Configs configs, Domain domain) {
    try {
      var nc = NatsUtils.createConnection(configs.NATS_URL);
      JetStreamManagement jsManagement = nc.jetStreamManagement();
      var commandRepo = new NatsCommandRepo(nc,
                                            configs.commandsTopic,
                                            configs.totalCommandPartitions,
                                            configs.transformer);
      var eventRepo = new NatsEventRepo(nc, configs.eventsTopic, configs.totalEventPartitions, configs.transformer);
      NatsUtils.createOrUpdateStream(jsManagement, configs.commandsTopic, 1);
      NatsUtils.createOrUpdateStream(jsManagement, configs.eventsTopic, 1);
      return new PartitionPipeline(domain, commandRepo, eventRepo, configs.eventPartition);
    } catch (IOException | InterruptedException | JetStreamApiException e) {
      throw new RuntimeException(e);
    }
  }

  public static PartitionPipeline inMemoryPipeline(Configs configs, Domain domain) {
    var commandRepo = CommandRepo.inMemory();
    var eventRepo = EventRepo.inMemory(configs.totalEventPartitions);
    return new PartitionPipeline(domain, commandRepo, eventRepo, configs.eventPartition);
  }
}
