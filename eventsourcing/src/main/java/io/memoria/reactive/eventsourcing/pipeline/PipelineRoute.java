package io.memoria.reactive.eventsourcing.pipeline;

public record PipelineRoute(String cmdTopic,
                            int cmdSubPartition,
                            int cmdTotalPubPartitions,
                            String eventTopic,
                            int eventSubPubPartition) {
  public PipelineRoute {
    if (cmdTopic == null || cmdTopic.isEmpty() || eventTopic == null || eventTopic.isEmpty()) {
      throw new IllegalArgumentException("Topic name can't be null or empty string");
    }
    if (cmdSubPartition < 0 || eventSubPubPartition < 0) {
      throw new IllegalArgumentException("Partition can't be less than 0");
    }
    if (cmdTotalPubPartitions < 1) {
      throw new IllegalArgumentException("Command Total partitions can't be less than 1");
    }
  }

  public String toShortString() {
    return "Route(%s_%d%%%d  -> %s_%d)".formatted(cmdTopic,
                                                  cmdSubPartition,
                                                  cmdTotalPubPartitions,
                                                  eventTopic,
                                                  eventSubPubPartition);
  }
}
