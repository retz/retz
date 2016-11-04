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
package io.github.retz.scheduler;

import io.github.retz.cli.FileConfiguration;
import org.junit.Test;

import java.net.URI;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Created by kuenishi on 2016/11/04.
 */
public class ServerConfigurationTest {
    @Test
    public void tryLoadConfig() throws Exception {
        ServerConfiguration config = new ServerConfiguration("src/test/resources/retz.properties");

        assertEquals(config.getMesosMaster(), "localhost:5050");
        assertEquals(config.getUri(), new URI("http://localhost:9090"));
        assertEquals(config.getKeystoreFile(), "path/to/keystore.jsk");
        assertEquals(config.getKeystorePass(), "foobar");
        assertFalse(config.checkCert());
        assertTrue(config.authenticationEnabled());
        assertEquals(0, config.getMaxStockSize());
    }
}
