/**
 *    Retz
 *    Copyright (C) 2016-2017 Nautilus Technologies, Inc.
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
package io.github.retz.scheduler;

import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.net.URI;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.*;

public class ServerConfigurationTest {
    @Test
    public void tryLoadConfig() throws Exception {
        ServerConfiguration config = new ServerConfiguration("src/test/resources/retz.properties");

        assertEquals(config.getMesosMaster(), "mesos.example.com:5050");
        assertEquals(config.getUri(), new URI("http://localhost:9090"));
        assertEquals(config.getKeystoreFile(), "path/to/keystore.jsk");
        assertEquals(config.getKeystorePass(), "foobar");
        assertTrue(config.insecure());
        assertTrue(config.authenticationEnabled());
        assertEquals(0, config.getMaxStockSize());
        assertEquals(7, config.getRefuseSeconds());
        assertEquals(42, config.getGcLeeway());
        assertEquals(3, config.getGcInterval());
        assertNotEquals("root", config.getUserName());

        assertEquals(42, config.getMaxListJobSize());
        assertEquals(12345, config.getMaxFileSize());
        assertEquals(false, config.getRecoverOnReconnect());
    }

    @Test(expected=IllegalArgumentException.class)
    public void wrongConfig() throws Exception {
        String s = "retz.mesos = localhost:5050\nretz.bind = http://0.0.0.0:9090\nretz.access.key = foobar\nretz.access.secret = bazbax";
        System.err.println(s);
        new ServerConfiguration(new ByteArrayInputStream(s.getBytes(UTF_8)));
    }

    @Test(expected=IllegalArgumentException.class)
    public void wrongConfig2() throws Exception {
        String s = "retz.mesos = mesos.example.com:5050\nretz.bind = http://example.com:90\nretz.access.key = foobar\nretz.access.secret = bazbax";
        System.err.println(s);
        new ServerConfiguration(new ByteArrayInputStream(s.getBytes(UTF_8)));
    }
}
