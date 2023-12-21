package io.memoria.reactive.testsuite;

import io.memoria.atom.core.id.Id;
import io.memoria.atom.eventsourcing.StateId;
import io.memoria.atom.testsuite.eventsourcing.AccountDecider;
import io.memoria.atom.testsuite.eventsourcing.AccountEvolver;
import io.memoria.atom.testsuite.eventsourcing.AccountSaga;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

public class TestUtils {
  // Rule
  public static final AtomicLong counter = new AtomicLong();
  public static final Supplier<Id> idSupplier = () -> Id.of(counter.getAndIncrement());
  public static final Supplier<Long> timeSupplier = () -> 0L;
  public static final AccountDecider decider = new AccountDecider(idSupplier, timeSupplier);
  public static final AccountEvolver evolver = new AccountEvolver();
  public static final AccountSaga saga = new AccountSaga(idSupplier, timeSupplier);

  // Data
  public static final String alice = "alice";
  public static StateId aliceId = StateId.of(alice);

  public static final String bob = "bob";
  public static StateId bobId = StateId.of(bob);

  private TestUtils() {}
}
