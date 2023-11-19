package io.memoria.reactive.eventsourcing.stream;

import io.memoria.reactive.eventsourcing.Event;
import io.memoria.reactive.eventsourcing.EventMeta;

record SomeEvent(EventMeta meta) implements Event {}