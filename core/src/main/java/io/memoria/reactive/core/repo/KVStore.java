package io.memoria.reactive.core.repo;

import reactor.core.publisher.Mono;

import java.util.Map;

public interface KVStore {
  Mono<String> get(String key);

  Mono<String> set(String key, String value);

  static KVStore inMemory() {
    return new MemKVStore();
  }

  static KVStore inMemory(Map<String, String> store) {
    return new MemKVStore(store);
  }
}
