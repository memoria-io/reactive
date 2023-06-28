package io.memoria.reactive.web.utils;

import io.vavr.Tuple;
import io.vavr.Tuple2;
import io.vavr.control.Option;

import java.util.Arrays;
import java.util.Base64;

import static io.vavr.control.Option.none;
import static io.vavr.control.Option.some;

public final class HttpUtils {

  private HttpUtils() {}

  public static Option<Tuple2<String, String>> basicCredentials(String header) {
    header = header.trim();
    if (header.contains("Basic")) {
      String content = header.split(" ")[1].trim();
      String[] basic = new String(Base64.getDecoder().decode(content)).split(":");
      return some(Tuple.of(basic[0], basic[1]));
    } else {
      return none();
    }
  }

  public static Option<String> bearerToken(String header) {
    header = header.trim();
    if (header.contains("Bearer")) {
      return some(header.split(" ")[1].trim());
    } else {
      return none();
    }
  }

  public static String joinPath(String... path) {
    return Arrays.stream(path).reduce("", (a, b) -> a + "/" + b).replace("//", "/");
  }
}
