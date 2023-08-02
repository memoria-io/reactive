package io.memoria.reactive.testsuite.eventsourcing.banking.stream;

import io.memoria.atom.eventsourcing.StateId;
import io.memoria.reactive.eventsourcing.stream.CommandStream;
import io.memoria.reactive.testsuite.eventsourcing.banking.BankingData;
import io.memoria.reactive.testsuite.eventsourcing.banking.domain.command.AccountCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;

public class CommandStreamScenario {
  private static final Logger log = LoggerFactory.getLogger(CommandStreamScenario.class.getName());
  private static final int initialBalance = 500;

  private final BankingData bankingData;
  private final CommandStream<AccountCommand> repo;
  private final int numOfAccounts;
  private final String topic;
  private final int partition;

  public CommandStreamScenario(BankingData bankingData,
                               CommandStream<AccountCommand> repo,
                               int numOfAccounts,
                               String topic,
                               int partition) {
    this.bankingData = bankingData;
    this.repo = repo;
    this.numOfAccounts = numOfAccounts;
    this.topic = topic;
    this.partition = partition;
  }

  public Flux<AccountCommand> publish() {
    var debitedIds = bankingData.createIds(0, numOfAccounts).map(StateId::of);
    var createDebitedAcc = bankingData.createAccountCmd(debitedIds, initialBalance);
    return createDebitedAcc.flatMap(msg -> repo.pub(topic, partition, msg));
  }

  public Flux<AccountCommand> subscribe() {
    return repo.sub(topic, partition);
  }
}
