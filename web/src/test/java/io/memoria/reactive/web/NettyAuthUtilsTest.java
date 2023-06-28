package io.memoria.reactive.web;

import io.memoria.reactive.web.auth.NettyAuthUtils;
import io.memoria.reactive.web.utils.NettyClientUtils;
import io.memoria.reactive.web.utils.NettyServerUtils;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import reactor.netty.DisposableServer;
import reactor.netty.http.server.HttpServerRoutes;
import reactor.test.StepVerifier;

import java.util.Base64;
import java.util.function.Consumer;

class NettyAuthUtilsTest {
  // basic and token only separated for simplicity, in production it should be one path
  private static final String basicAuthPath = "/authenticate_basic";
  private static final String tokenAuthPath = "/authenticate_token";
  private static final DisposableServer disposableServer;

  static {
    disposableServer = TestUtils.httpServer.route(routes()).bindNow();
  }

  @Test
  @DisplayName("Should deserialize Basic authorization header correctly")
  void basicFromTest() {
    var basic = Base64.getEncoder().encodeToString(("bob:password").getBytes());
    Consumer<HttpHeaders> headers = b -> b.add("Authorization", "Basic " + basic);
    var monoResp = NettyClientUtils.post(TestUtils.httpClient.headers(headers), basicAuthPath, "payload hello");
    StepVerifier.create(monoResp)
                .expectNext(Response.of(HttpResponseStatus.OK, "(bob, password)"))
                .expectComplete()
                .verify();
  }

  @Test
  @DisplayName("Should deserialize bearer authorization header correctly")
  void tokenFromTest() {
    var token = "xyz.xyz.xyz";
    Consumer<HttpHeaders> httpHeaders = b -> b.add("Authorization", "Bearer " + token);
    var monoResp = NettyClientUtils.get(TestUtils.httpClient.headers(httpHeaders), tokenAuthPath);
    StepVerifier.create(monoResp).expectNext(Response.of(HttpResponseStatus.OK, token)).expectComplete().verify();
  }

  @AfterAll
  static void afterAll() {
    disposableServer.dispose();
  }

  private static Consumer<HttpServerRoutes> routes() {
    return r -> r.get(tokenAuthPath,
                      (req, resp) -> NettyServerUtils.stringReply.apply(resp)
                                                                 .apply(HttpResponseStatus.OK,
                                                                        NettyAuthUtils.bearerToken(req).get()))
                 .post(basicAuthPath,
                       (req, resp) -> NettyServerUtils.stringReply.apply(resp)
                                                                  .apply(HttpResponseStatus.OK,
                                                                         NettyAuthUtils.basicCredentials(req)
                                                                                       .get()
                                                                                       .toString()));
  }
}
