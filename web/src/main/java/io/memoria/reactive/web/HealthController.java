package io.memoria.reactive.web;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.vavr.Function2;
import reactor.core.publisher.Mono;
import reactor.netty.http.server.HttpServerRequest;
import reactor.netty.http.server.HttpServerResponse;
import reactor.util.Logger;
import reactor.util.Loggers;

public record HealthController(Mono<String> check)
        implements Function2<HttpServerRequest, HttpServerResponse, Mono<Void>> {
  private static final Logger log = Loggers.getLogger(HealthController.class.getName());

  public Mono<Void> apply(HttpServerRequest req, HttpServerResponse resp) {
    return check.doOnNext(msg -> log.info("Health check succeeded: %s".formatted(msg)))
                .doOnError(t -> log.error("Health check failed: %s".formatted(t.getMessage())))
                .doOnError(t -> log.debug("Health check failed:", t))
                .flatMap(msg -> resp.status(HttpResponseStatus.OK).sendString(Mono.just(msg)).then())
                .onErrorResume(e -> resp.status(HttpResponseStatus.INTERNAL_SERVER_ERROR)
                                        .sendString(Mono.just(e.getMessage()))
                                        .then());
  }
}
