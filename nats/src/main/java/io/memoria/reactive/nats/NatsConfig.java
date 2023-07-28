package io.memoria.reactive.nats;

import io.nats.client.api.StorageType;

import java.time.Duration;

public record NatsConfig(String url,
                         StorageType storageType,
                         int replicas,
                         int fetchBatchSize,
                         Duration fetchMaxWait,
                         Duration fetchLastMaxWait,
                         boolean denyDelete,
                         boolean denyPurge) {

  public static NatsConfig appendOnly(String url,
                                      StorageType storageType,
                                      int replicationFactor,
                                      int fetchBatch,
                                      Duration fetchOnceMaxWait,
                                      Duration fetchLastMsgMaxWait) {
    return new NatsConfig(url,
                          storageType,
                          replicationFactor,
                          fetchBatch,
                          fetchOnceMaxWait,
                          fetchLastMsgMaxWait,
                          true,
                          true);
  }
}
