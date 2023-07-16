package io.memoria.reactive.eventsourcing.testsuite.banking.scenario;

import io.memoria.atom.core.id.Id;
import io.memoria.reactive.eventsourcing.testsuite.banking.domain.command.AccountCommand;
import io.memoria.reactive.eventsourcing.testsuite.banking.domain.command.ChangeName;
import io.memoria.reactive.eventsourcing.testsuite.banking.domain.command.CloseAccount;
import io.memoria.reactive.eventsourcing.testsuite.banking.domain.command.CreateAccount;
import io.memoria.reactive.eventsourcing.testsuite.banking.domain.command.Credit;
import io.memoria.reactive.eventsourcing.testsuite.banking.domain.command.Debit;
import io.vavr.collection.List;
import io.vavr.collection.Map;

import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

public class Data {
  public final String namePrefix;
  public final AtomicLong counter;
  public final Supplier<Id> idSupplier;
  public final Supplier<Long> timeSupplier;

  public static Data ofSerial(String namePrefix) {
    return new Data(namePrefix);
  }

  public static Data ofUUID(String namePrefix) {
    return of(namePrefix, new AtomicLong(), Id::of, () -> 0L);
  }

  public static Data of(String namePrefix, AtomicLong counter, Supplier<Id> idSupplier, Supplier<Long> timeSupplier) {
    return new Data(namePrefix, counter, idSupplier, timeSupplier);
  }

  public Id createId(int i) {
    return Id.of(namePrefix + i);
  }

  public List<Id> createIds(int from, int to) {
    return List.range(from, to).map(this::createId);
  }

  public List<Id> createShuffledIds(int nAccounts) {
    return List.range(0, nAccounts).shuffle().map(this::createId);
  }

  public String createName(int nameVersion) {
    return namePrefix + nameVersion;
  }

  public CreateAccount createAccountCmd(Id id, long balance) {
    return new CreateAccount(idSupplier.get(), id, timeSupplier.get(), createName(0), balance);
  }

  public List<AccountCommand> createAccountCmd(List<Id> ids, long balance) {
    return ids.map(id -> createAccountCmd(id, balance));
  }

  public List<AccountCommand> changeNameCmd(List<Id> ids, int version) {
    return ids.map(id -> new ChangeName(idSupplier.get(), id, timeSupplier.get(), createName(version)));
  }

  public Credit creditCmd(Id debited, int balance, Id id) {
    return new Credit(idSupplier.get(), id, timeSupplier.get(), debited, balance);
  }

  public List<AccountCommand> creditCmd(List<Id> credited, Id debited, int balance) {
    return credited.map(id -> creditCmd(debited, balance, id)).map(i -> (AccountCommand) i).toList();
  }

  public List<AccountCommand> creditCmd(Map<Id, Id> creditedDebited, int balance) {
    return creditedDebited.map(entry -> creditCmd(entry._2, balance, entry._1)).map(i -> (AccountCommand) i).toList();
  }

  public Debit debitCmd(Id debited, Id credited, int amount) {
    return new Debit(idSupplier.get(), debited, timeSupplier.get(), credited, amount);
  }

  public List<AccountCommand> debitCmd(Map<Id, Id> debitedCredited, int amount) {
    return debitedCredited.map(entry -> debitCmd(entry._1, entry._2, amount)).map(i -> (AccountCommand) i).toList();
  }

  public List<AccountCommand> randomDebitAmountCmd(Map<Id, Id> debitedCredited, int maxAmount) {
    var random = new Random();
    return debitedCredited.map(entry -> debitCmd(entry._1, entry._2, random.nextInt(maxAmount)))
                          .map(i -> (AccountCommand) i)
                          .toList();
  }

  public CloseAccount closeAccountCmd(Id i) {
    return new CloseAccount(idSupplier.get(), i, timeSupplier.get());
  }

  public List<AccountCommand> toCloseAccountCmd(List<Id> ids) {
    return ids.map(this::closeAccountCmd);
  }

  private Data(String namePrefix) {
    this.namePrefix = namePrefix;
    this.counter = new AtomicLong();
    this.idSupplier = () -> Id.of(counter.getAndIncrement());
    this.timeSupplier = counter::getAndIncrement;
  }

  private Data(String namePrefix, AtomicLong counter, Supplier<Id> idSupplier, Supplier<Long> timeSupplier) {
    this.namePrefix = namePrefix;
    this.counter = counter;
    this.idSupplier = idSupplier;
    this.timeSupplier = timeSupplier;
  }
}
