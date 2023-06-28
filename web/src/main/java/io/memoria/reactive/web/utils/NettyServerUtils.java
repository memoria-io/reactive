package io.memoria.reactive.web.utils;

import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vavr.Function2;
import io.vavr.Function3;
import io.vavr.Function4;
import reactor.core.publisher.Mono;
import reactor.netty.NettyOutbound;
import reactor.netty.http.server.HttpServerResponse;

public class NettyServerUtils {
  public static final Function4<HttpServerResponse, HttpResponseStatus, HttpHeaders, String, NettyOutbound> reply = (resp, status, headers, message) -> resp.status(
          status).headers(resp.responseHeaders().add(headers)).sendString(Mono.just(message));

  public static final Function3<HttpServerResponse, HttpResponseStatus, String, NettyOutbound> stringReply = (resp, status, message) -> resp.status(
          status).sendString(Mono.just(message));

  public static final Function2<HttpServerResponse, HttpResponseStatus, NettyOutbound> statusReply = (resp, status) -> resp.status(
          status).sendString(Mono.just(status.reasonPhrase()));

  private NettyServerUtils() {}
}
