package io.memoria.reactive.core.reactor;

import io.vavr.collection.List;
import io.vavr.control.Either;
import io.vavr.control.Option;
import io.vavr.control.Try;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.io.IOException;
import java.time.Duration;
import java.util.Random;
import java.util.function.Function;

class ReactorUtilsTest {
  private static final Random r = new Random();

  @Test
  void callableToMono() {
    var v = ReactorUtils.callableToMono(() -> "hello world", new Exception("isFalse"));
    StepVerifier.create(v.apply(true)).expectNext("hello world").expectComplete().verify();
    StepVerifier.create(v.apply(false)).expectErrorMessage("isFalse").verify();
  }

  @Test
  void callableToVoidMono() {
    var v = ReactorUtils.toVoidMono(() -> {}, new Exception("isFalse"));
    StepVerifier.create(v.apply(true)).expectComplete().verify();
    StepVerifier.create(v.apply(false)).expectErrorMessage("isFalse").verify();
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void booleanToMonoPosNeg(boolean value) {
    var m = ReactorUtils.booleanToMono(value, Mono.just(true), Mono.just(false));
    StepVerifier.create(m).expectNext(value).verifyComplete();
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void booleanToMonoPositive(boolean value) {
    var m = ReactorUtils.booleanToMono(value, Mono.just(true));
    if (value) {
      StepVerifier.create(m).expectNext(true).verifyComplete();
    } else {
      StepVerifier.create(m).expectComplete().verify();
    }
  }

  @Test
  void eitherToMonoTest() {
    Either<Exception, Integer> k = Either.right(23);
    Mono<Integer> integerMono = ReactorUtils.eitherToMono(k);
    StepVerifier.create(integerMono).expectNext(23).expectComplete().verify();

    k = Either.left(new Exception("exception example"));
    integerMono = ReactorUtils.eitherToMono(k);
    StepVerifier.create(integerMono).expectError().verify();
  }

  @Test
  void toMonoFromOption() {
    // Given
    var som = Option.some(20);
    var non = Option.none();
    // When
    var somMono = ReactorUtils.optionToMono(som, new IllegalArgumentException("not found"));
    var nonMono = ReactorUtils.optionToMono(non, new IllegalArgumentException("not found"));
    // Then
    StepVerifier.create(somMono).expectNext(20).expectComplete().verify();
    StepVerifier.create(nonMono).expectError(IllegalArgumentException.class).verify();
  }

  @Test
  void tryListToFlux() {
    var t = Try.success(List.of(1, 2, 3));
    var f = ReactorUtils.toFlux(t);
    StepVerifier.create(f).expectNext(1, 2, 3).expectComplete().verify();
    var te = Try.<List<Integer>>failure(new IOException());
    var fe = ReactorUtils.toFlux(te);
    StepVerifier.create(fe).expectError(IOException.class);
  }

  @Test
  void tryToMonoTest() {
    var tSuccess = Try.ofSupplier(() -> "hello");
    StepVerifier.create(ReactorUtils.tryToMono(() -> tSuccess)).expectNext("hello").expectComplete().verify();
    var tFailure = Try.failure(new Exception("Exception Happened"));
    StepVerifier.create(ReactorUtils.tryToMono(() -> tFailure)).expectError(Exception.class).verify();
  }

  @Test
  void tryToMonoVoidTest() {
    Mono<Try<String>> original = Mono.just(Try.success("one"));
    Function<String, Mono<Void>> deferredOp = (String content) -> Mono.empty();
    Function<Throwable, Mono<Void>> throwable = t -> Mono.just(Try.failure(new Exception("should not fail"))).then();
    Mono<Void> voidMono = original.flatMap(ReactorUtils.toVoidMono(deferredOp, throwable));
    StepVerifier.create(voidMono).expectComplete().verify();
  }

  @Test
  @Disabled("A debugging test")
  void acc() {
    var f = Flux.just(1, 2, 3, 4);
    StepVerifier.create(f.flatMap(this::hi).doOnNext(System.out::println)).expectNextCount(3).verifyComplete();
  }

  private int getRandom() {
    var k = r.nextInt(1000);
    System.out.println("Random is called:" + k);
    return k;
  }

  private Mono<Integer> hi(int i) {
    System.out.println("hi is called");
    if (i < 2) {
      return Mono.empty();
    } else {
      return Mono.just(i).delayElement(Duration.ofMillis(getRandom()));
    }
  }
}
