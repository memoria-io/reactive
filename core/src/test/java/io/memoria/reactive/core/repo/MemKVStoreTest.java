package io.memoria.reactive.core.repo;

import io.vavr.collection.List;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

class MemKVStoreTest {
  private final KVStore kvStore = KVStore.inMemory();

  @Test
  void getAndPut() {
    // Given
    int count = 1000;
    var expectedValues = List.range(0, count).map(MemKVStoreTest::toValue).toJavaArray(String[]::new);
    var setKV = Flux.range(0, count).concatMap(i -> kvStore.set(toKey(i), toValue(i)));

    // Then
    StepVerifier.create(setKV).expectNextCount(count).verifyComplete();
    StepVerifier.create(Flux.range(0, count).concatMap(i -> kvStore.get(toKey(i))))
                .expectNext(expectedValues)
                .verifyComplete();
  }

  private static String toKey(Integer i) {
    return "key:" + i;
  }

  private static String toValue(Integer i) {
    return "value:" + i;
  }
}
