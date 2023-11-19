package io.memoria.reactive.testsuite.state;

import io.memoria.reactive.eventsourcing.State;

public sealed interface Account extends State permits OpenAccount, ClosedAccount {}
