package io.memoria.reactive.eventsourcing.stream;

import io.memoria.reactive.eventsourcing.Command;
import io.memoria.reactive.eventsourcing.CommandMeta;

record SomeCommand(CommandMeta meta) implements Command {}
