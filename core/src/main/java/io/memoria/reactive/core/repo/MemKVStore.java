package io.memoria.reactive.core.repo;

import io.vavr.control.Option;
import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

class MemKVStore implements KVStore {
  private final Map<String, String> store;

  public MemKVStore() {
    store = new ConcurrentHashMap<>();
  }

  public MemKVStore(Map<String, String> store) {
    this.store = store;
  }

  @Override
  public Mono<String> get(String key) {
    return Mono.fromCallable(() -> Option.of(store.get(key))).filter(Option::isDefined).map(Option::get);
  }

  @Override
  public Mono<String> set(String key, String value) {
    return Mono.fromCallable(() -> {
      store.computeIfPresent(key, (k, v) -> value);
      store.computeIfAbsent(key, k -> value);
      return value;
    });
  }
}
