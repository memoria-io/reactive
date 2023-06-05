package io.memoria.reactive.eventsourcing.exception;

import io.memoria.atom.core.id.Id;
import io.memoria.reactive.eventsourcing.Command;
import io.memoria.reactive.eventsourcing.Event;
import io.memoria.reactive.eventsourcing.State;

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
      var msg = "Invalid creator event:%s for creating state, this should never happen";
      return new InvalidEvent(msg.formatted(event.getClass().getSimpleName()));
    }

    public static InvalidEvent of(State state, Event event) {
      var msg = "Invalid evolution of: %s on current state: %s, this should never happen";
      return new InvalidEvent(msg.formatted(state.getClass().getSimpleName(), event.getClass().getSimpleName()));
    }
  }

  class MismatchingStateId extends Exception implements ESException {
    private MismatchingStateId(String msg) {
      super(msg);
    }

    public static MismatchingStateId of(Id stateId, Id cmdStateId) {
      var msg = "The Command's stateId:%s doesn't match stream stateId:%s";
      return new MismatchingStateId(msg.formatted(cmdStateId.value(), stateId.value()));
    }
  }
}
