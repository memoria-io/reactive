package io.memoria.reactive.core.vavr;

import io.vavr.collection.List;
import io.vavr.control.Either;
import io.vavr.control.Option;
import io.vavr.control.Try;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.io.IOException;
import java.util.function.Function;

class ReactorVavrUtilsTest {

  @Test
  void booleanToMono() {
    var v = ReactorVavrUtils.callableToMono(() -> "hello world", new Exception("isFalse"));
    StepVerifier.create(v.apply(true)).expectNext("hello world").expectComplete().verify();
    StepVerifier.create(v.apply(false)).expectErrorMessage("isFalse").verify();
  }

  @Test
  void booleanToVoidMono() {
    var v = ReactorVavrUtils.toVoidMono(() -> {}, new Exception("isFalse"));
    StepVerifier.create(v.apply(true)).expectComplete().verify();
    StepVerifier.create(v.apply(false)).expectErrorMessage("isFalse").verify();
  }

  @Test
  void eitherToMonoTest() {
    Either<Exception, Integer> k = Either.right(23);
    Mono<Integer> integerMono = ReactorVavrUtils.eitherToMono(k);
    StepVerifier.create(integerMono).expectNext(23).expectComplete().verify();

    k = Either.left(new Exception("exception example"));
    integerMono = ReactorVavrUtils.eitherToMono(k);
    StepVerifier.create(integerMono).expectError().verify();
  }

  @Test
  void toMonoFromOption() {
    // Given
    var som = Option.some(20);
    var non = Option.none();
    // When
    var somMono = ReactorVavrUtils.optionToMono(som, new IllegalArgumentException("not found"));
    var nonMono = ReactorVavrUtils.optionToMono(non, new IllegalArgumentException("not found"));
    // Then
    StepVerifier.create(somMono).expectNext(20).expectComplete().verify();
    StepVerifier.create(nonMono).expectError(IllegalArgumentException.class).verify();
  }

  @Test
  void tryListToFlux() {
    var t = Try.success(List.of(1, 2, 3));
    var f = ReactorVavrUtils.toFlux(t);
    StepVerifier.create(f).expectNext(1, 2, 3).expectComplete().verify();
    var te = Try.<List<Integer>>failure(new IOException());
    var fe = ReactorVavrUtils.toFlux(te);
    StepVerifier.create(fe).expectError(IOException.class);
  }

  @Test
  void tryToMonoTest() {
    var tSuccess = Try.ofSupplier(() -> "hello");
    StepVerifier.create(ReactorVavrUtils.tryToMono(() -> tSuccess)).expectNext("hello").expectComplete().verify();
    var tFailure = Try.failure(new Exception("Exception Happened"));
    StepVerifier.create(ReactorVavrUtils.tryToMono(() -> tFailure)).expectError(Exception.class).verify();
  }

  @Test
  void tryToMonoVoidTest() {
    Mono<Try<String>> original = Mono.just(Try.success("one"));
    Function<String, Mono<Void>> deferredOp = (String content) -> Mono.empty();
    Function<Throwable, Mono<Void>> throwable = t -> Mono.just(Try.failure(new Exception("should not fail"))).then();
    Mono<Void> voidMono = original.flatMap(ReactorVavrUtils.toVoidMono(deferredOp, throwable));
    StepVerifier.create(voidMono).expectComplete().verify();
  }
}
