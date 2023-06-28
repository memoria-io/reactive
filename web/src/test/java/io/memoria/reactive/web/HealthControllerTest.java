package io.memoria.reactive.web;

import io.memoria.reactive.web.observability.HealthController;
import io.memoria.reactive.web.utils.NettyClientUtils;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.netty.DisposableServer;
import reactor.netty.http.server.HttpServerRoutes;
import reactor.test.StepVerifier;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static io.memoria.reactive.web.TestUtils.httpClient;
import static io.memoria.reactive.web.TestUtils.httpServer;

class HealthControllerTest {
  private static final AtomicBoolean flip = new AtomicBoolean(true);
  private static final String ERROR_MSG = "The flip was false!";
  private static final String endpoint = "/health";
  private static final HealthController healthCheck;
  private static final DisposableServer disposableServer;

  static {
    healthCheck = new HealthController(check());
    disposableServer = httpServer.route(routes()).bindNow();
  }

  @Test
  void failure() {
    // Given
    flip.set(false);
    var monoResp = NettyClientUtils.get(httpClient, endpoint);
    // Then
    StepVerifier.create(monoResp)
                .expectNext(Response.of(HttpResponseStatus.INTERNAL_SERVER_ERROR, ERROR_MSG))
                .verifyComplete();
  }

  @Test
  void success() {
    // Given
    flip.set(true);
    var monoResp = NettyClientUtils.get(httpClient, endpoint);
    // Then
    StepVerifier.create(monoResp).expectNext(Response.of(HttpResponseStatus.OK, "ok")).verifyComplete();
  }

  @AfterAll
  static void afterAll() {
    disposableServer.dispose();
  }

  static Consumer<HttpServerRoutes> routes() {
    return r -> r.get(endpoint, healthCheck);
  }

  private static Mono<String> check() {
    return Mono.fromCallable(flip::get).flatMap(b -> (b) ? Mono.just("ok") : Mono.error(new Exception(ERROR_MSG)));
  }
}
