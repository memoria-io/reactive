package io.memoria.reactive.web.auth;

import io.memoria.reactive.web.utils.HttpUtils;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpHeaders;
import io.vavr.Tuple2;
import io.vavr.control.Option;
import reactor.netty.http.server.HttpServerRequest;

public class NettyAuthUtils {
  public static final HttpHeaders AUTH_CHALLENGES = new DefaultHttpHeaders().add("WWW-Authenticate", "Basic, Bearer");

  private NettyAuthUtils() {}

  public static Option<Tuple2<String, String>> basicCredentials(HttpServerRequest req) {
    return Option.of(req.requestHeaders().get("Authorization")).flatMap(HttpUtils::basicCredentials);
  }

  public static Option<String> bearerToken(HttpServerRequest req) {
    return Option.of(req.requestHeaders().get("Authorization")).flatMap(HttpUtils::bearerToken);
  }
}
