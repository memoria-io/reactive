package io.memoria.reactive.web;

import io.memoria.reactive.web.utils.NettyClientUtils;
import io.memoria.reactive.web.utils.NettyServerUtils;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import reactor.netty.DisposableServer;
import reactor.netty.http.server.HttpServerRoutes;
import reactor.test.StepVerifier;

import java.util.function.Consumer;

import static io.memoria.reactive.web.TestUtils.httpClient;

class NettyServerUtilsTest {
  private static final String stringReplyPath = "/string";
  private static final String statusReplyPath = "/status";
  private static final String errorReplyPath = "/error";
  private static final DisposableServer disposableServer;

  static {
    disposableServer = TestUtils.httpServer.route(routes()).bindNow();
  }

  @Test
  void errorReplyTest() {
    var monoResp = NettyClientUtils.get(httpClient, errorReplyPath);
    StepVerifier.create(monoResp)
                .expectNext(Response.of(HttpResponseStatus.UNAUTHORIZED, "Unauthorized"))
                .expectComplete()
                .verify();
  }

  @Test
  void statusReplyTest() {
    var monoResp = NettyClientUtils.get(httpClient, statusReplyPath);
    StepVerifier.create(monoResp)
                .expectNext(Response.of(HttpResponseStatus.UNAUTHORIZED,
                                        HttpResponseStatus.UNAUTHORIZED.reasonPhrase()))
                .expectComplete()
                .verify();
  }

  @Test
  void stringReplyTest() {
    var monoResp = NettyClientUtils.get(httpClient, stringReplyPath);
    StepVerifier.create(monoResp).expectNext(Response.of(HttpResponseStatus.OK, "Hello")).expectComplete().verify();
  }

  @AfterAll
  static void afterAll() {
    disposableServer.dispose();
  }

  private static Consumer<HttpServerRoutes> routes() {
    return r -> r.get(statusReplyPath,
                      (req, resp) -> NettyServerUtils.statusReply.apply(resp).apply(HttpResponseStatus.UNAUTHORIZED))
                 .get(stringReplyPath,
                      (req, resp) -> NettyServerUtils.stringReply.apply(resp).apply(HttpResponseStatus.OK, "Hello"))
                 .get(errorReplyPath,
                      (req, resp) -> NettyServerUtils.stringReply.apply(resp)
                                                                 .apply(HttpResponseStatus.UNAUTHORIZED,
                                                                        HttpResponseStatus.UNAUTHORIZED.reasonPhrase()));
  }
}
