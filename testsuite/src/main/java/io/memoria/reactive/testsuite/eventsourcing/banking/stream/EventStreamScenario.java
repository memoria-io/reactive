package io.memoria.reactive.testsuite.eventsourcing.banking.stream;

import io.memoria.reactive.eventsourcing.stream.EventStream;
import io.memoria.reactive.testsuite.eventsourcing.banking.BankingData;
import io.memoria.reactive.testsuite.eventsourcing.banking.domain.event.AccountEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;

public class EventStreamScenario {
  private static final Logger log = LoggerFactory.getLogger(EventStreamScenario.class.getName());
  private static final int initialBalance = 500;

  private final BankingData bankingData;
  private final EventStream<AccountEvent> repo;
  private final int numOfAccounts;
  private final String topic;
  private final int partition;

  public EventStreamScenario(BankingData bankingData,
                             EventStream<AccountEvent> repo,
                             int numOfAccounts,
                             String topic,
                             int partition) {
    this.bankingData = bankingData;
    this.repo = repo;
    this.numOfAccounts = numOfAccounts;
    this.topic = topic;
    this.partition = partition;
  }

  public Flux<AccountEvent> publish() {
    var debitedIds = bankingData.createIds(0, numOfAccounts);
    var createDebitedAcc = bankingData.createAccountEvent(debitedIds, initialBalance);
    return createDebitedAcc.flatMap(msg -> repo.pub(topic, partition, msg));
  }

  public Flux<AccountEvent> subscribe() {
    return repo.sub(topic, partition);
  }
}
