package io.memoria.reactive.web;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.vavr.Function2;
import reactor.core.publisher.Mono;
import reactor.netty.http.server.HttpServerRequest;
import reactor.netty.http.server.HttpServerResponse;

public record VarzController() implements Function2<HttpServerRequest, HttpServerResponse, Mono<Void>> {
  public Mono<Void> apply(HttpServerRequest req, HttpServerResponse resp) {
    return resp.status(HttpResponseStatus.OK).sendString(Mono.just(HttpResponseStatus.OK.reasonPhrase())).then();
  }
}
