package io.memoria.reactive.web;

import reactor.netty.http.client.HttpClient;
import reactor.netty.http.server.HttpServer;

public class TestUtils {
  static final String host = "127.0.0.1";
  static final int port = 8081;
  static final HttpServer httpServer;
  static final HttpClient httpClient;

  static {
    httpServer = HttpServer.create().host(host).port(port);
    httpClient = HttpClient.create().host(host).port(port);
  }
}
