package io.memoria.reactive.web;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.ByteBufFlux;
import reactor.netty.ByteBufMono;
import reactor.netty.http.client.HttpClient;
import reactor.netty.http.client.HttpClientResponse;

public class NettyClientUtils {
  private NettyClientUtils() {}

  public static Mono<Response> delete(HttpClient httpClient, String path) {
    return httpClient.delete().uri(path).responseSingle(NettyClientUtils::response);
  }

  public static Mono<Response> response(HttpClientResponse res, ByteBufMono body) {
    return body.asString().defaultIfEmpty("").map(s -> Response.of(res.status(), s));
  }

  public static Mono<Response> get(HttpClient httpClient, String path) {
    return httpClient.get().uri(path).responseSingle(NettyClientUtils::response);
  }

  public static Mono<Response> post(HttpClient httpClient, String path, String payload) {
    return httpClient.post()
                     .uri(path)
                     .send(ByteBufFlux.fromString(Flux.just(payload)))
                     .responseSingle(NettyClientUtils::response);
  }

  public static Mono<Response> put(HttpClient httpClient, String path, String payload) {
    return httpClient.put()
                     .uri(path)
                     .send(ByteBufFlux.fromString(Flux.just(payload)))
                     .responseSingle(NettyClientUtils::response);
  }
}
