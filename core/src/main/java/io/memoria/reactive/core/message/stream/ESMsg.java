package io.memoria.reactive.core.message.stream;

import java.io.Serializable;

public record ESMsg(String key, String value) implements Serializable {}
