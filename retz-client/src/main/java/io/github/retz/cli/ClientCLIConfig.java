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

import io.github.retz.protocol.data.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Objects;

public class ClientCLIConfig extends FileConfiguration {
    static final Logger LOG = LoggerFactory.getLogger(ClientCLIConfig.class);

    private final String RETZ_SERVER_URI = "retz.server.uri";
    private final URI retzServerUri;

    public ClientCLIConfig(String file) throws IOException, URISyntaxException {
        super(file);
        retzServerUri = new URI(Objects.requireNonNull(super.properties.getProperty(RETZ_SERVER_URI)));
        LOG.debug("Client only config: {}={}", RETZ_SERVER_URI, retzServerUri);
    }

    public ClientCLIConfig(ClientCLIConfig c) {
        super(c.properties);
        retzServerUri = c.getUri();
    }

    public URI getUri() {
        return retzServerUri;
    }

    // For test purpose in retz-server
    public void setUser(User u) {
        properties.setProperty(ACCESS_KEY, u.keyId());
        properties.setProperty(ACCESS_SECRET, u.secret());
    }

    @Override
    public String toString() {
        return super.toString() + RETZ_SERVER_URI + "=" + retzServerUri;
    }
}
