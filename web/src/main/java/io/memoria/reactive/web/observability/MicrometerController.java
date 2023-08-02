package io.memoria.reactive.web.observability;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.vavr.Function2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.netty.http.server.HttpServerRequest;
import reactor.netty.http.server.HttpServerResponse;

import java.util.function.Supplier;

public class MicrometerController implements Function2<HttpServerRequest, HttpServerResponse, Mono<Void>> {
  private static final Logger log = LoggerFactory.getLogger(MicrometerController.class.getName());
  private final transient Supplier<String> response;

  public MicrometerController(Supplier<String> response) {
    this.response = response;
  }

  public Mono<Void> apply(HttpServerRequest req, HttpServerResponse resp) {

    return Mono.fromSupplier(response)
               .doOnError(t -> log.error("Micrometer scrape failed: %s".formatted(t.getMessage())))
               .doOnError(t -> log.debug("Micrometer scrape failed:", t))
               .flatMap(msg -> resp.status(HttpResponseStatus.OK).sendString(Mono.just(msg)).then())
               .onErrorResume(e -> resp.status(HttpResponseStatus.INTERNAL_SERVER_ERROR)
                                       .sendString(Mono.just(e.getMessage()))
                                       .then());
  }
}
