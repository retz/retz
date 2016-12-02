/**
 *    Retz
 *    Copyright (C) 2016 Nautilus Technologies, Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package io.github.retz.cli;

import io.github.retz.auth.Authenticator;
import org.junit.Test;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.io.File;
import java.net.URI;
import java.util.Base64;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class FileConfigurationTest {
    @Test
    public void tryLoadConfig() throws Exception {
        FileConfiguration config = new FileConfiguration("src/test/resources/retz.properties");

        assertEquals(config.getKeystoreFile(), "path/to/keystore.jsk");
        assertEquals(config.getKeystorePass(), "foobar");
        assertFalse(config.checkCert());
        assertTrue(config.authenticationEnabled());
        assertEquals(40822, config.getJmxPort());
    }

    @Test
    public void signature() throws  Exception {
        FileConfiguration config = new FileConfiguration("src/test/resources/retz.properties");

        assertTrue(config.authenticationEnabled());
        Authenticator a = config.getAuthenticator();
        String verb = "GET";
        String md5 = "1234567890";
        String date = TimestampHelper.now();
        String resource = "/job/234";
        String signature = sign(verb, md5, date, resource, "deadbeef");
        System.err.println("Signature: "+signature);
        assertTrue(a.authenticate(verb, md5, date, resource, "cafebabe", signature));

        String wrongSignature = sign(verb, md5, date, resource, "my name is charlie");
        System.err.println("Wrong signature: "+wrongSignature);
        assertFalse(a.authenticate(verb, md5, date, resource, "cafebabe", wrongSignature));
    }

    private String sign(String verb, String md5, String date, String resource, String secret) throws Exception {
    String string2sign = new StringBuilder()
            .append(verb).append("\n")
            .append(md5).append("\n")
            .append(date).append("\n")
            .append(resource).append("\n")
            .toString();
        Mac mac = Mac.getInstance("HmacSHA256");

        mac.init(new SecretKeySpec(secret.getBytes(UTF_8), "HmacSHA256"));

        return Base64.getEncoder().withoutPadding().encodeToString(mac.doFinal(string2sign.getBytes(UTF_8)));
    }
}
