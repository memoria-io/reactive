package io.memoria.reactive.testsuite.state;

import io.memoria.reactive.eventsourcing.StateMeta;

public record ClosedAccount(StateMeta meta) implements Account {}
