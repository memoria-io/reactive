package io.memoria.reactive.web;

import io.memoria.reactive.web.utils.HttpUtils;
import io.vavr.Tuple;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Base64;

import static io.vavr.control.Option.none;
import static io.vavr.control.Option.some;

class HttpUtilsTest {

  @Test
  void basicExtraSpacesFail() {
    var header = "Basic  " + Base64.getEncoder().encodeToString(("bob" + ":" + "password").getBytes());
    Assertions.assertThrows(ArrayIndexOutOfBoundsException.class, () -> HttpUtils.basicCredentials(header));
  }

  @Test
  void basicExtraSpacesSuccess() {
    var header = "   Basic " + Base64.getEncoder().encodeToString(("bob" + ":" + "password").getBytes()) + "   ";
    var t = HttpUtils.basicCredentials(header);
    Assertions.assertEquals(Tuple.of("bob", "password"), t.get());
  }

  @Test
  void basicNoColonFail() {
    var header = "   Basic " + Base64.getEncoder().encodeToString(("bob" + "" + "password").getBytes()) + "   ";
    Assertions.assertThrows(ArrayIndexOutOfBoundsException.class, () -> HttpUtils.basicCredentials(header));
  }

  @Test
  void basicNoIdSuccess() {
    var header = "   Basic " + Base64.getEncoder().encodeToString(("" + ":" + "password").getBytes()) + "   ";
    var t = HttpUtils.basicCredentials(header);
    Assertions.assertEquals(Tuple.of("", "password"), t.get());
  }

  @Test
  void basicNoPasswordFail() {
    var header = "   Basic " + Base64.getEncoder().encodeToString(("bob" + ":" + "").getBytes()) + "   ";
    Assertions.assertThrows(ArrayIndexOutOfBoundsException.class, () -> HttpUtils.basicCredentials(header));
  }

  @Test
  void basicSuccess() {
    var header = "Basic " + Base64.getEncoder().encodeToString(("bob" + ":" + "password").getBytes());
    var t = HttpUtils.basicCredentials(header);
    Assertions.assertEquals(Tuple.of("bob", "password"), t.get());
  }

  @Test
  void bearerSuccess() {
    var token = "xyz.xyz.zyz";
    var header = "Bearer " + token;
    Assertions.assertEquals(some(token), HttpUtils.bearerToken(header));
  }

  @Test
  void noBasic() {
    var header = "   Base " + Base64.getEncoder().encodeToString(("bob" + "" + "password").getBytes()) + "   ";
    Assertions.assertEquals(none(), HttpUtils.basicCredentials(header));
  }

  @Test
  void noBearer() {
    var token = "xyz.xyz.zyz";
    var header = "Bearr " + token;
    Assertions.assertEquals(none(), HttpUtils.bearerToken(header));
  }
}
