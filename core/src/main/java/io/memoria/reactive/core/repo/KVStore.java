package io.memoria.reactive.core.repo;

import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public interface KVStore {
  Mono<String> get(String key);

  Mono<String> set(String key, String value);

  static KVStore inMemory() {
    return new MemKVStore(new ConcurrentHashMap<>());
  }

  static KVStore inMemory(Map<String, String> store) {
    return new MemKVStore(store);
  }
}
