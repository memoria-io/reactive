package io.memoria.reactive.eventsourcing;

public interface Shardable {
  default boolean isInPartition(int partition, int totalPartitions) {
    return partition == partition(totalPartitions);
  }

  default int partition(int totalPartitions) {
    return partition(stateId(), totalPartitions);
  }

  /**
   * @return the stateId which is used as sharding key as well.
   */
  StateId stateId();

  static int partition(StateId stateId, int totalPartitions) {
    var hash = (stateId.hashCode() == Integer.MIN_VALUE) ? Integer.MAX_VALUE : stateId.hashCode();
    return Math.abs(hash) % totalPartitions;
  }
}
