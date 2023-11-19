package io.memoria.reactive.testsuite;

import io.memoria.atom.core.id.Id;
import io.memoria.atom.eventsourcing.StateId;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

public class TestUtils {
  // Rule
  public static final AtomicLong counter = new AtomicLong();
  public static final Supplier<Id> idSupplier = () -> Id.of(counter.getAndIncrement());
  public static final Supplier<Long> timeSupplier = () -> 0L;
  public static final io.memoria.reactive.testsuite.AccountDecider decider = new AccountDecider(idSupplier,
                                                                                                timeSupplier);
  public static final io.memoria.reactive.testsuite.AccountEvolver evolver = new AccountEvolver();
  public static final io.memoria.reactive.testsuite.AccountSaga saga = new AccountSaga(idSupplier, timeSupplier);

  // Data
  public static final String alice = "alice";
  public static StateId aliceId = StateId.of(alice);

  public static final String bob = "bob";
  public static StateId bobId = StateId.of(bob);

  private TestUtils() {}
}
