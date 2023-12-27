package io.memoria.reactive.testsuite;

import io.memoria.atom.core.id.Id;
import io.memoria.atom.eventsourcing.CommandId;
import io.memoria.atom.eventsourcing.CommandMeta;
import io.memoria.atom.eventsourcing.Domain;
import io.memoria.atom.eventsourcing.StateId;
import io.memoria.atom.testsuite.eventsourcing.AccountDecider;
import io.memoria.atom.testsuite.eventsourcing.AccountEvolver;
import io.memoria.atom.testsuite.eventsourcing.AccountSaga;
import io.memoria.atom.testsuite.eventsourcing.command.AccountCommand;
import io.memoria.atom.testsuite.eventsourcing.command.ChangeName;
import io.memoria.atom.testsuite.eventsourcing.command.CloseAccount;
import io.memoria.atom.testsuite.eventsourcing.command.CreateAccount;
import io.memoria.atom.testsuite.eventsourcing.command.Debit;
import reactor.core.publisher.Flux;
import reactor.util.function.Tuple2;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

public class Data {
  // Data
  public static final String alice = "alice";
  public static StateId aliceId = StateId.of(alice);
  public static final String bob = "bob";
  public static StateId bobId = StateId.of(bob);
  public final Supplier<Id> idSupplier;
  public final Supplier<Long> timeSupplier;
  public final AccountSaga saga;
  public final AccountDecider decider;
  public final AccountEvolver evolver;

  private Data(Supplier<Id> idSupplier, Supplier<Long> timeSupplier) {
    this.idSupplier = idSupplier;
    this.timeSupplier = timeSupplier;
    saga = new AccountSaga(idSupplier, timeSupplier);
    decider = new AccountDecider(idSupplier, timeSupplier);
    evolver = new AccountEvolver();
  }

  public Domain domain() {
    return new Domain(new AccountDecider(idSupplier, timeSupplier),
                      new AccountEvolver(),
                      new AccountSaga(idSupplier, timeSupplier));
  }

  public static Data ofSerial() {
    var counter = new AtomicLong();
    return new Data(() -> Id.of(counter.getAndIncrement()), () -> 0L);
  }

  public static Data ofUUID() {
    return new Data(() -> Id.of(UUID.randomUUID()), System::currentTimeMillis);
  }

  public Id createId(int i) {
    return Id.of(i);
  }

  public Flux<Id> createIds(int from, int to) {
    return Flux.range(from, to).map(this::createId);
  }

  public CreateAccount createAccountCmd(StateId stateId, long balance) {
    return new CreateAccount(createCommandMeta(stateId), stateId.value(), balance);
  }

  public Flux<AccountCommand> createAccountCmd(Flux<StateId> stateIds, long balance) {
    return stateIds.map(id -> createAccountCmd(id, balance));
  }

  public Flux<AccountCommand> changeNameCmd(Flux<StateId> stateIds, int version) {
    return stateIds.map(stateId -> new ChangeName(createCommandMeta(stateId), String.valueOf(version)));
  }

  public Debit debitCmd(StateId debited, StateId credited, int amount) {
    return new Debit(createCommandMeta(debited), credited, amount);
  }

  public Flux<AccountCommand> debitCmd(Flux<Tuple2<StateId, StateId>> debitedCredited, int amount) {
    return debitedCredited.map(entry -> debitCmd(entry.getT1(), entry.getT2(), amount));
  }

  public CloseAccount closeAccountCmd(StateId stateId) {
    return new CloseAccount(createCommandMeta(stateId));
  }

  public Flux<AccountCommand> closeAccounts(Flux<StateId> stateIds) {
    return stateIds.map(this::closeAccountCmd);
  }

  private CommandMeta createCommandMeta(StateId stateId) {
    return new CommandMeta(CommandId.of(idSupplier.get()), stateId, timeSupplier.get());
  }
}
