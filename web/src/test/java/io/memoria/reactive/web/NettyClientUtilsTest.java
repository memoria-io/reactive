package io.memoria.reactive.web;

import io.netty.handler.codec.http.HttpResponseStatus;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.netty.DisposableServer;
import reactor.netty.http.server.HttpServerRequest;
import reactor.netty.http.server.HttpServerResponse;
import reactor.netty.http.server.HttpServerRoutes;
import reactor.test.StepVerifier;

import java.util.function.Consumer;

import static io.memoria.reactive.web.TestUtils.httpClient;
import static io.memoria.reactive.web.TestUtils.httpServer;

class NettyClientUtilsTest {
  private static final DisposableServer disposableServer;
  static final String ping = "ping";
  static final String pong = "pong";
  static final String endpoint = "/endpoint";

  static {
    disposableServer = httpServer.route(routes()).bindNow();
  }

  @Test
  void deleteTest() {
    var monoResp = NettyClientUtils.delete(httpClient, endpoint);
    StepVerifier.create(monoResp).expectNext(Response.of(HttpResponseStatus.OK, pong)).expectComplete().verify();
  }

  @Test
  void getTest() {
    var monoResp = NettyClientUtils.get(httpClient, endpoint);
    StepVerifier.create(monoResp).expectNext(Response.of(HttpResponseStatus.OK, pong)).expectComplete().verify();
  }

  @Test
  void notFoundTest() {
    var monoResp = NettyClientUtils.post(httpClient, "/someUndefinedPath", ping);
    StepVerifier.create(monoResp).expectNext(Response.of(HttpResponseStatus.NOT_FOUND, "")).expectComplete().verify();
  }

  @Test
  void postTest() {
    var monoResp = NettyClientUtils.post(httpClient, endpoint, ping);
    StepVerifier.create(monoResp).expectNext(Response.of(HttpResponseStatus.OK, ping + pong)).expectComplete().verify();
  }

  @Test
  void putTest() {
    var monoResp = NettyClientUtils.put(httpClient, endpoint, ping);
    StepVerifier.create(monoResp).expectNext(Response.of(HttpResponseStatus.OK, ping + pong)).expectComplete().verify();
  }

  @AfterAll
  static void afterAll() {
    disposableServer.dispose();
  }

  static Consumer<HttpServerRoutes> routes() {
    return r -> r.get(endpoint, NettyClientUtilsTest::handle)
                 .post(endpoint, NettyClientUtilsTest::handlePayload)
                 .put(endpoint, NettyClientUtilsTest::handlePayload)
                 .delete(endpoint, NettyClientUtilsTest::handle);
  }

  private static Mono<Void> handle(HttpServerRequest req, HttpServerResponse resp) {
    return NettyServerUtils.stringReply.apply(resp).apply(HttpResponseStatus.OK).apply(pong).then();
  }

  private static Mono<Void> handlePayload(HttpServerRequest req, HttpServerResponse resp) {
    return req.receive()
              .aggregate()
              .asString()
              .flatMap(ping -> NettyServerUtils.stringReply.apply(resp)
                                                           .apply(HttpResponseStatus.OK)
                                                           .apply(ping + pong)
                                                           .then());
  }
}
