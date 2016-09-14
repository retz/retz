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

import org.junit.Test;

import java.io.File;
import java.net.URI;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * Created by kuenishi on 9/14/16.
 */
public class FileConfigurationTest {
    @Test
    public void tryLoadConfig() throws Exception {
        FileConfiguration config = new FileConfiguration("src/test/resources/retz.properties");

        assertEquals(config.getMesosMaster(), "123.23.43.356:23563");
        assertEquals(config.getUri(), new URI("https://234.34.34.2:23452"));
        assertEquals(config.getKeystoreFile(), "path/to/keystore.jsk");
        assertEquals(config.getKeystorePass(), "foobar");
        assertFalse(config.checkCert());
    }
}
