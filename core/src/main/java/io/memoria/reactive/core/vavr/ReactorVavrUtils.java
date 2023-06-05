package io.memoria.reactive.core.vavr;

import io.vavr.API;
import io.vavr.Patterns;
import io.vavr.collection.List;
import io.vavr.control.Either;
import io.vavr.control.Option;
import io.vavr.control.Try;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.concurrent.Callable;
import java.util.function.Function;
import java.util.function.Supplier;

import static java.lang.Boolean.TRUE;
import static java.util.function.Function.identity;

public final class ReactorVavrUtils {
  private ReactorVavrUtils() {}

  //---------------------------------------------------------------------------------------------------------
  // To Mono
  //---------------------------------------------------------------------------------------------------------
  public static Function<Boolean, Mono<Void>> toVoidMono(Runnable t, Throwable throwable) {
    return b -> TRUE.equals(b) ? Mono.fromRunnable(t) : Mono.error(throwable);
  }

  public static <A> Function<Try<A>, Mono<Void>> toVoidMono(Function<A, Mono<Void>> f,
                                                            Function<Throwable, Mono<Void>> f2) {
    return a -> API.Match(a).of(API.Case(Patterns.$Success(API.$()), f), API.Case(Patterns.$Failure(API.$()), f2));
  }

  public static <T> Function<Boolean, Mono<T>> callableToMono(Callable<T> t, Throwable throwable) {
    return b -> TRUE.equals(b) ? Mono.fromCallable(t) : Mono.error(throwable);
  }

  //---------------------------------------------------------------------------------------------------------
  // Option To Mono
  //---------------------------------------------------------------------------------------------------------

  public static <T> Mono<T> optionToMono(Option<T> t) {
    return Mono.fromCallable(() -> t).flatMap(opt -> (opt.isDefined()) ? Mono.just(opt.get()) : Mono.empty());
  }

  public static <T> Mono<T> optionToMono(Option<T> t, Throwable throwable) {
    return Mono.fromCallable(() -> t).flatMap(opt -> (opt.isDefined()) ? Mono.just(opt.get()) : Mono.error(throwable));
  }

  //---------------------------------------------------------------------------------------------------------
  // Try to Mono
  //---------------------------------------------------------------------------------------------------------

  public static <L extends Throwable, R> Mono<R> eitherToMono(Either<L, R> t) {
    return Mono.fromCallable(() -> t)
               .flatMap(opt -> (opt.isRight()) ? Mono.just(opt.get()) : Mono.error(opt.getLeft()));
  }

  public static <T> Mono<T> tryToMono(Supplier<Try<T>> t) {
    return Mono.fromSupplier(t).flatMap(tr -> {
      if (tr.isSuccess()) {
        return Mono.just(tr.get());
      } else {
        return Mono.error(tr.getCause());
      }
    });
  }

  //---------------------------------------------------------------------------------------------------------
  // To Flux
  //---------------------------------------------------------------------------------------------------------

  public static <T> Flux<T> toFlux(Try<List<T>> tr) {
    return Mono.fromCallable(() -> tr.isSuccess() ? Flux.fromIterable(tr.get()) : Flux.<T>error(tr.getCause()))
               .flatMapMany(identity());
  }
}
