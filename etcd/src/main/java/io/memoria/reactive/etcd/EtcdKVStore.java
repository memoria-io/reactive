package io.memoria.reactive.etcd;

import io.etcd.jetcd.*;
import io.memoria.reactive.core.repo.KVStore;
import io.vavr.control.Option;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.util.List;

public class EtcdKVStore implements KVStore {
  private final KV kv;

  public EtcdKVStore(Client client) {
    kv = client.getKVClient();
  }

  @Override
  public Mono<String> get(String key) {
    return Mono.fromFuture(kv.get(toByteSequence(key)))
               .map(getResp -> toValue(getResp.getKvs()))
               .filter(Option::isDefined)
               .map(Option::get);
  }

  @Override
  public Mono<String> set(String key, String value) {
    return Mono.fromFuture(kv.put(toByteSequence(key), toByteSequence(value))).map(putResp -> value);
  }

  private static ByteSequence toByteSequence(String value) {
    return ByteSequence.from(value, StandardCharsets.UTF_8);
  }

  private static Option<String> toValue(List<KeyValue> kvs) {
    return Option.ofOptional(kvs.stream().findFirst()).map(kv -> String.valueOf(kv.getValue()));
  }
}
