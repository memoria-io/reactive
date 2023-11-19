package io.memoria.reactive.eventsourcing;

public interface ESException {
  class InvalidCommand extends IllegalArgumentException implements ESException {
    private InvalidCommand(String msg) {
      super(msg);
    }

    public static InvalidCommand of(Command command) {
      var msg = "Invalid initializer command (%s)".formatted(command.getClass().getSimpleName());
      return new InvalidCommand(msg);
    }

    public static InvalidCommand of(State state, Command command) {
      var msg = "Invalid command (%s) for the state (%s)".formatted(command.getClass().getSimpleName(),
                                                                    state.getClass().getSimpleName());
      return new InvalidCommand(msg);
    }
  }

  class InvalidEvent extends IllegalArgumentException implements ESException {

    private InvalidEvent(String msg) {
      super(msg);
    }

    public static InvalidEvent of(Event event) {
      var msg = "Invalid creator event: %s[%s] for creating state, this should never happen";
      return new InvalidEvent(msg.formatted(event.getClass().getSimpleName(), event.meta()));
    }

    public static InvalidEvent of(State state, Event event) {
      var msg = "Invalid evolution event: %s[%s] to the state: %s[%s], this should never happen";
      return new InvalidEvent(msg.formatted(event.getClass().getSimpleName(),
                                            event.meta(),
                                            state.getClass().getSimpleName(),
                                            state.meta()));
    }
  }

  class MismatchingStateId extends IllegalArgumentException implements ESException {
    private MismatchingStateId(String msg) {
      super(msg);
    }

    public static MismatchingStateId of(StateId stateId, StateId cmdStateId) {
      var msg = "The Command's stateId:%s doesn't match stateId:%s";
      return new MismatchingStateId(msg.formatted(cmdStateId.value(), stateId.value()));
    }
  }
}
