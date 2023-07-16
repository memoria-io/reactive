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
  private static final AtomicLong atomic = new AtomicLong();
  public static final String NAME_PREFIX = "name_version:";
  public static final Supplier<Id> idSupplier = () -> Id.of(atomic.getAndIncrement());
  public static final Supplier<Long> timeSupplier = () -> 0L;

  public static Id createId(int i) {
    return Id.of("acc_id_" + i);
  }

  public static List<Id> createIds(int from, int to) {
    return List.range(from, to).map(Data::createId);
  }

  public static List<Id> createShuffledIds(int nAccounts) {
    return List.range(0, nAccounts).shuffle().map(Data::createId);
  }

  public static String createName(int nameVersion) {
    return NAME_PREFIX + nameVersion;
  }

  public static CreateAccount newCreateAccountCmd(Id id, long balance) {
    return new CreateAccount(idSupplier.get(), id, timeSupplier.get(), createName(0), balance);
  }

  public static List<AccountCommand> newCreateAccountCmd(List<Id> ids, long balance) {
    return ids.map(id -> newCreateAccountCmd(id, balance));
  }

  public static List<AccountCommand> newChangeNameCmd(List<Id> ids, int version) {
    return ids.map(id -> new ChangeName(idSupplier.get(), id, timeSupplier.get(), createName(version)));
  }

  public static Credit newCreditCmd(Id debited, int balance, Id id) {
    return new Credit(idSupplier.get(), id, timeSupplier.get(), debited, balance);
  }

  public static List<AccountCommand> newCreditCmd(List<Id> credited, Id debited, int balance) {
    return credited.map(id -> newCreditCmd(debited, balance, id)).map(i -> (AccountCommand) i).toList();
  }

  public static List<AccountCommand> newCreditCmd(Map<Id, Id> creditedDebited, int balance) {
    return creditedDebited.map(entry -> newCreditCmd(entry._2, balance, entry._1))
                          .map(i -> (AccountCommand) i)
                          .toList();
  }

  public static Debit newDebitCmd(Id debited, Id credited, int amount) {
    return new Debit(idSupplier.get(), debited, timeSupplier.get(), credited, amount);
  }

  public static List<AccountCommand> newDebitCmd(Map<Id, Id> debitedCredited, int amount) {
    return debitedCredited.map(entry -> newDebitCmd(entry._1, entry._2, amount)).map(i -> (AccountCommand) i).toList();
  }

  public static List<AccountCommand> newRandomDebitAmountCmd(Map<Id, Id> debitedCredited, int maxAmount) {
    var random = new Random();
    return debitedCredited.map(entry -> newDebitCmd(entry._1, entry._2, random.nextInt(maxAmount)))
                          .map(i -> (AccountCommand) i)
                          .toList();
  }

  public static CloseAccount newCloseAccountCmd(Id i) {
    return new CloseAccount(idSupplier.get(), i, timeSupplier.get());
  }

  public static List<AccountCommand> toCloseAccountCmd(List<Id> ids) {
    return ids.map(Data::newCloseAccountCmd);
  }

  private Data() {}
}
