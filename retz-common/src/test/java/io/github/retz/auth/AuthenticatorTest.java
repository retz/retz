package io.github.retz.auth;

import io.github.retz.cli.TableFormatter;
import io.github.retz.cli.TimestampHelper;
import org.junit.Test;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.security.MessageDigest;
import java.util.Base64;
import java.util.Calendar;
import java.util.Date;
import java.util.function.Function;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;

public class AuthenticatorTest {
    private static final String ALGORITHM = "HmacSHA256";

    @Test
    public void microBenchHashings() throws Exception {
        String secret = "deadbeef secret";
        Authenticator auth = new Authenticator("deadbeef key", secret);
        String string2sign = auth.string2sign("PUT", "0xcafebabe", TimestampHelper.now(), "/");

        TableFormatter formatter = new TableFormatter("module", "time(ms)", "generated hash");

        encode(formatter, "javax.crypto.Mac", (Void t) -> {
            SecretKeySpec SECRET_KEY_SPEC;
            SECRET_KEY_SPEC = new SecretKeySpec(secret.getBytes(UTF_8), ALGORITHM);
            try {
                Mac mac = Mac.getInstance(ALGORITHM);
                mac.init(SECRET_KEY_SPEC);

                byte[] mac_bytes = mac.doFinal(string2sign.getBytes(UTF_8));
                return Base64.getEncoder().withoutPadding().encodeToString(mac_bytes);
            } catch (Exception e) {
                return "fail" + e.toString();
            }
        });


    encode(formatter, "java.security.MessageDigest", (Void t) -> {

        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");

            md.update(secret.getBytes(UTF_8));
            md.update(string2sign.getBytes(UTF_8));
            return Base64.getEncoder().withoutPadding().encodeToString(md.digest());

        } catch (Exception cnse) {
            return "fail" + cnse.toString();
        }
    });

        System.err.println(formatter.titles());
        for (String line : formatter) {
            System.err.println(line);
        }
    }

    public void encode(TableFormatter formatter, String name, Function<Void, String> func) {
        Date start = Calendar.getInstance().getTime();
        String result = func.apply(null);
        assertFalse(result.startsWith("fail"));
        Date end = Calendar.getInstance().getTime();
        long diffms = end.getTime() - start.getTime();
        formatter.feed("javax.crypto.Mac", Long.toString(diffms), result);
    }

    @Test
    public void diff() throws Exception {
        String secret = "foobar";
        {
            String string2sign = "cafebabe";
            MessageDigest md = MessageDigest.getInstance("SHA-256");

            md.update(secret.getBytes(UTF_8));
            md.update(string2sign.getBytes(UTF_8));
            System.err.println(Base64.getEncoder().withoutPadding().encodeToString(md.digest()));
        }
        {
            String string2sign = "pocketburger";
            MessageDigest md = MessageDigest.getInstance("SHA-256");

            md.update(secret.getBytes(UTF_8));
            md.update(string2sign.getBytes(UTF_8));
            System.err.println(Base64.getEncoder().withoutPadding().encodeToString(md.digest()));
        }
    }
}
