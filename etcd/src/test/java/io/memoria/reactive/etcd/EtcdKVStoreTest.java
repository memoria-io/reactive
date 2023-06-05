package io.memoria.reactive.etcd;

import io.etcd.jetcd.Client;
import io.vavr.collection.List;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.util.Random;

class EtcdKVStoreTest {
  private final Random random = new Random();
  private final String keyPrefix = "key_" + random.nextInt(1000);
  private final Client client = Client.builder().endpoints("http://localhost:2379").build();
  private final EtcdKVStore kvStore = new EtcdKVStore(client);

  @Test
  void getAndPut() {
    // Given
    int count = 1000;
    var expectedValues = List.range(0, count).map(this::toValue).toJavaArray(String[]::new);
    var putKeyFlux = Flux.range(0, count).concatMap(i -> kvStore.set(toKey(i), toValue(i)));

    // Then
    StepVerifier.create(putKeyFlux).expectNextCount(count).verifyComplete();
    StepVerifier.create(Flux.range(0, count).concatMap(i -> kvStore.get(toKey(i))))
                .expectNext(expectedValues)
                .verifyComplete();
  }

  @Test
  void notFound() {
    StepVerifier.create(kvStore.get("some_value")).expectComplete();
  }

  private String toKey(int i) {
    return keyPrefix + "_" + i;
  }

  private String toValue(int i) {
    return "value:" + i;
  }
}
