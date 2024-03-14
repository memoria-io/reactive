package io.memoria.reactive.eventsourcing.pipeline;

import io.memoria.atom.core.text.TextTransformer;
import io.memoria.atom.eventsourcing.Domain;
import io.memoria.atom.eventsourcing.command.Command;
import io.memoria.atom.eventsourcing.command.CommandId;
import io.memoria.atom.eventsourcing.event.Event;
import io.memoria.atom.eventsourcing.event.EventId;
import io.memoria.atom.eventsourcing.state.State;
import io.memoria.atom.eventsourcing.state.StateId;
import io.memoria.reactive.eventsourcing.stream.Msg;
import io.memoria.reactive.eventsourcing.stream.MsgStream;
import io.vavr.control.Option;
import io.vavr.control.Try;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static io.memoria.reactive.core.reactor.ReactorUtils.tryToMono;

public class PartitionPipeline {
  private static final Logger log = LoggerFactory.getLogger(PartitionPipeline.class.getName());

  // Core
  private final Domain domain;

  // Infra
  private final CommandRoute commandRoute;
  private final EventRoute eventRoute;
  private final MsgStream msgStream;
  private final Duration initGracePeriod;
  private final TextTransformer transformer;

  // In memory
  private final Map<StateId, State> aggregates;
  private final Set<CommandId> processedCommands;
  private final Set<EventId> sagaSources;
  private final AtomicReference<EventId> prevEvent;

  /**
   * The Ids cache (sagaSources and processedCommands) size of 1Million ~= 16Megabyte, since UUID is 32bit -> 16byte
   */
  public PartitionPipeline(Domain domain,
                           CommandRoute commandRoute,
                           EventRoute eventRoute,
                           MsgStream msgStream,
                           Duration initGracePeriod,
                           TextTransformer transformer) {
    // Core
    this.domain = domain;

    // Infra
    this.commandRoute = commandRoute;
    this.eventRoute = eventRoute;
    this.msgStream = msgStream;
    this.initGracePeriod = initGracePeriod;
    this.transformer = transformer;

    // In memory
    this.aggregates = new HashMap<>();
    this.processedCommands = new HashSet<>();
    this.sagaSources = new HashSet<>();
    this.prevEvent = new AtomicReference<>();
  }

  public Flux<Event> start() {
    return initialize(initGracePeriod).concatWith(verifyInitialization()).concatWith(handle(subscribeToCommands()));
  }

  /**
   * Load previous events and build the state only
   *
   * @param initGraceDuration amount of time to leave before considering no further initialisation events would arrive
   */
  public Flux<Event> initialize(Duration initGraceDuration) {
    return subscribeToEventsUntil(initGraceDuration).filter(this::isValidEvent)
                                                    .doOnNext(this::addSaga)
                                                    .doOnNext(this::evolve);
  }

  public Mono<Event> verifyInitialization() {
    return msgStream.last(eventRoute.topic(), eventRoute.partition())
                    .flatMap(msg -> tryToMono(() -> toEvent(msg)))
                    .flatMap(this::verifyLastEvent);
  }

  private Mono<Event> verifyLastEvent(Event lastEvent) {
    if (lastEvent.meta().eventId().equals(getPrevEvent().get())) {
      return Mono.empty();
    } else {
      return Mono.error(UnexpectedLastEvent.of(lastEvent));
    }
  }

  /**
   * Handle specific commands only
   */
  public Flux<Event> handle(Flux<Command> commands) {
    return commands.concatMap(this::redirectIfNeeded)
                   .filter(this::isValidCommand)
                   .concatMap(this::decide)
                   .doOnNext(this::addSaga)
                   .doOnNext(this::evolve)
                   .concatMap(this::saga)
                   .concatMap(this::publishEvent);
  }

  public Domain getDomain() {
    return domain;
  }

  public CommandRoute getCommandRoute() {
    return commandRoute;
  }

  public EventRoute getEventRoute() {
    return eventRoute;
  }

  public TextTransformer getTransformer() {
    return transformer;
  }

  public Option<EventId> getPrevEvent() {
    return Option.of(prevEvent.get());
  }

  public Flux<Event> subscribeToEvents() {
    return msgStream.sub(eventRoute.topic(), eventRoute.partition()).concatMap(msg -> tryToMono(() -> toEvent(msg)));
  }

  public Flux<Event> subscribeToEventsUntil(Duration timeout) {
    return msgStream.subUntil(eventRoute.topic(), eventRoute.partition(), timeout)
                    .concatMap(msg -> tryToMono(() -> toEvent(msg)));
  }

  public Flux<Command> subscribeToCommands() {
    return msgStream.sub(commandRoute.topic(), commandRoute.partition())
                    .concatMap(msg -> tryToMono(() -> toCommand(msg)));
  }

  public Mono<Msg> publishCommand(Command command) {
    return tryToMono(() -> toMsg(command)).flatMap(msgStream::pub);
  }

  public Mono<Event> decide(Command cmd) {
    return Mono.defer(() -> {

      cmd.meta().sagaSource().forEach(sagaSources::add);
      if (aggregates.containsKey(cmd.meta().stateId())) {
        return tryToMono(() -> domain.decider().apply(aggregates.get(cmd.meta().stateId()), cmd));
      } else {
        return tryToMono(() -> domain.decider().apply(cmd));
      }
    });
  }

  public boolean isValidEvent(Event event) {
    if (prevEvent.get() != null && prevEvent.get().equals(event.meta().eventId())) {
      var message = "Redelivered event[%s], ignoring...".formatted(event.meta());
      log.debug(message);
      return false;
    } else {
      return true;
    }
  }

  public boolean isValidCommand(Command cmd) {
    var alreadyProcessedCmd = processedCommands.contains(cmd.meta().commandId());
    var alreadyProcessedSagaCmd = cmd.meta().sagaSource().map(sagaSources::contains).getOrElse(false);
    return !alreadyProcessedCmd && !alreadyProcessedSagaCmd;
  }

  public Try<Command> toCommand(Msg msg) {
    return transformer.deserialize(msg.value(), Command.class);
  }

  public Try<Event> toEvent(Msg msg) {
    return transformer.deserialize(msg.value(), Event.class);
  }

  public Try<Msg> toMsg(Event event) {
    return transformer.serialize(event)
                      .map(payload -> new Msg(eventRoute.topic(),
                                              eventRoute.partition(),
                                              event.meta().eventId().value(),
                                              payload));
  }

  public Try<Msg> toMsg(Command command) {
    var partition = command.partition(commandRoute.totalPartitions());
    return transformer.serialize(command)
                      .map(payload -> new Msg(commandRoute.topic(),
                                              partition,
                                              command.meta().commandId().value(),
                                              payload));
  }

  /**
   * Has side effect only available with package visibility
   */
  Mono<Event> publishEvent(Event event) {
    return tryToMono(() -> toMsg(event)).flatMap(msgStream::pub).map(_ -> event);
  }

  /**
   * Redirection allows location transparency and auto sharding
   * <p>
   * Has side effect only available with package visibility
   */
  Mono<Command> redirectIfNeeded(Command cmd) {
    if (cmd.meta().isInPartition(commandRoute.partition(), commandRoute.totalPartitions())) {
      return Mono.just(cmd);
    } else {
      return publishCommand(cmd).flatMap(_ -> Mono.empty());
    }
  }

  /**
   * Has side effect only available with package visibility
   */
  Mono<Event> saga(Event e) {
    return Mono.defer(() -> {
      var opt = domain.saga().apply(e);
      if (opt.isDefined()) {
        return publishCommand(opt.get()).map(_ -> e);
      } else {
        return Mono.just(e);
      }
    });
  }

  void addSaga(Event event) {
    event.meta().sagaSource().forEach(sagaSources::add);
  }

  /**
   * Has side effect
   */
  void evolve(Event event) {
    if (isInitializerEvent(event)) {
      update(event, domain.evolver().apply(event));
      return;
    }
    if (isEvolutionEvent(event)) {
      update(event, domain.evolver().apply(aggregates.get(event.meta().stateId()), event));
      return;
    }
    throw new IllegalArgumentException("Unknown event:[%s]".formatted(event));
  }

  /**
   * Has side effect
   */
  void update(Event e, State newState) {
    aggregates.put(e.meta().stateId(), newState);
    processedCommands.add(e.meta().commandId());
    prevEvent.set(e.meta().eventId());
  }

  private boolean isInitializerEvent(Event event) {
    boolean versionIsZero = event.meta().version() == 0;
    boolean stateExists = aggregates.containsKey(event.meta().stateId());
    return versionIsZero && !stateExists;
  }

  private boolean isEvolutionEvent(Event event) {
    boolean stateExists = aggregates.containsKey(event.meta().stateId());
    return stateExists && event.meta().version() == aggregates.get(event.meta().stateId()).meta().version() + 1;
  }
}
