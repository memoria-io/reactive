package io.memoria.reactive.nats.eventsourcing;

import io.memoria.reactive.core.stream.ESMsg;
import io.nats.client.api.StorageType;
import io.vavr.Tuple;
import io.vavr.Tuple2;

import java.time.Duration;
import java.util.Objects;

public record TopicConfig(String topic,
                          int partition,
                          StorageType storageType,
                          int replicas,
                          int fetchBatchSize,
                          Duration fetchMaxWait,
                          boolean denyDelete,
                          boolean denyPurge) {

  public static final String SPLIT_TOKEN = "_";
  public static final String SUBJECT_EXT = ".subject";

  public TopicConfig {
    if (topic == null || topic.isEmpty()) {
      throw new IllegalArgumentException("Topic name can't be null or empty string");
    }
    if (partition < 0) {
      throw new IllegalArgumentException("Partition can't be less than 0");
    }
    if (replicas < 1 || replicas > 5) {
      throw new IllegalArgumentException("Replicas must be from 1 to 5 inclusive.");
    }
    if (fetchBatchSize < 1) {
      throw new IllegalArgumentException("fetchBatchSize can't be less than 1");
    }
  }

  public String streamName() {
    return streamName(topic, partition);
  }

  public String subjectName() {
    return subjectName(topic, partition);
  }

  public static String streamName(String topic, int partition) {
    return "%s%s%d".formatted(topic, SPLIT_TOKEN, partition);
  }

  public static String subjectName(String topic, int partition) {
    return streamName(topic, partition) + SUBJECT_EXT;
  }

  public static String toSubjectName(ESMsg msg) {
    return subjectName(msg.topic(), msg.partition());
  }

  public static Tuple2<String, Integer> topicPartition(String subject) {
    var idx = subject.indexOf(SUBJECT_EXT);
    var s = subject.substring(0, idx).split(SPLIT_TOKEN);
    var topic = s[0];
    var partition = Integer.parseInt(s[1]);
    return Tuple.of(topic, partition);
  }

  public static TopicConfig appendOnly(String topic,
                                       int partition,
                                       StorageType storageType,
                                       int replicationFactor,
                                       int fetchBatch,
                                       Duration fetchMaxWait) {
    return new TopicConfig(topic, partition, storageType, replicationFactor, fetchBatch, fetchMaxWait, true, true);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this)
      return true;
    if (obj == null || obj.getClass() != this.getClass())
      return false;
    var that = (TopicConfig) obj;
    return Objects.equals(this.topic, that.topic) && this.partition == that.partition;
  }

  @Override
  public int hashCode() {
    return Objects.hash(topic, partition);
  }

  @Override
  public String toString() {
    return "TopicConfig["
           + "topic="
           + topic
           + ", "
           + "partition="
           + partition
           + ", "
           + "storageType="
           + storageType
           + ", "
           + "replicationFactor="
           + replicas
           + ", "
           + "fetchBatchSize="
           + fetchBatchSize
           + ", "
           + "fetchMaxWait="
           + fetchMaxWait
           + ", "
           + "denyDelete="
           + denyDelete
           + ", "
           + "denyPurge="
           + denyPurge
           + ']';
  }
}
