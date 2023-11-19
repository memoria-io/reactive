package io.memoria.reactive.testsuite.state;

import io.memoria.atom.eventsourcing.StateMeta;

public record OpenAccount(StateMeta meta,
                          String name,
                          long balance,
                          int debitCount,
                          int confirmedDebit,
                          int creditCount) implements Account {
  public OpenAccount(StateMeta meta, String name, long balance) {
    this(meta, name, balance, 0, 0, 0);
  }

  public boolean hasOngoingDebit() {
    return debitCount != confirmedDebit;
  }

  public OpenAccount withCredit(long credit) {
    return new OpenAccount(meta.incrementVersion(),
                           name,
                           balance + credit,
                           debitCount,
                           confirmedDebit,
                           creditCount + 1);
  }

  public OpenAccount withDebit(long debit) {
    return new OpenAccount(meta.incrementVersion(), name, balance - debit, debitCount + 1, confirmedDebit, creditCount);
  }

  public OpenAccount withDebitConfirmed() {
    return new OpenAccount(meta.incrementVersion(), name, balance, debitCount, confirmedDebit + 1, creditCount);
  }

  public OpenAccount withDebitRejected(long returnedDebit) {
    return new OpenAccount(meta.incrementVersion(),
                           name,
                           balance + returnedDebit,
                           debitCount - 1,
                           confirmedDebit,
                           creditCount);
  }

  public OpenAccount withName(String newName) {
    return new OpenAccount(meta.incrementVersion(), newName, balance, debitCount, confirmedDebit, creditCount);
  }

  public boolean canDebit(long amount) {
    return balance - amount > 0;
  }
}
