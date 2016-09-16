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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Objects;
import java.util.Properties;

public class FileConfiguration {
    private static final Logger LOG = LoggerFactory.getLogger(FileConfiguration.class);

    static final String MESOS_LOC_KEY = "retz.mesos";
    static final String BIND_ADDRESS = "retz.bind";
    static final String QUEUE_MAX = "retz.queue.max";
    static final String MESOS_ROLE = "retz.mesos.role";
    static final String MESOS_PRINCIPAL = "retz.mesos.principal";
    static final String USE_GPU = "retz.gpu";
    static final String ACCESS_SECRET = "retz.secret";
    static final String SCHEDULE_RESULTS = "retz.results";
    static final String SCHEDULE_RETRY = "retz.retry";

    // https://github.com/apache/mesos/blob/master/include/mesos/mesos.proto#L208-L210
    static final String USER_NAME = "retz.user";
    static final String ZK_SERVERS = "retz.zk.servers";
    
    static final String DEFAULT_MESOS_PRINCIPAL = "retz";

    private final Properties properties;
    private final URI uri;
    private final boolean useGPU;

    public FileConfiguration(String path) throws IOException, URISyntaxException {
        this(new File(path));
    }

    public FileConfiguration(File file) throws IOException, URISyntaxException {
        this(new FileInputStream(file));
        LOG.debug("Parsed file {}: {}", file.getName(),
                String.join(", ", properties.stringPropertyNames()));
    }

    public FileConfiguration(InputStream in) throws IOException, URISyntaxException{
        this.properties = new Properties();
        properties.load(in);
        in.close();

        Objects.requireNonNull(properties.getProperty(MESOS_LOC_KEY), "Mesos master location cannot be empty");
        Objects.requireNonNull(properties.getProperty(BIND_ADDRESS), "Host and port are required");

        uri = new URI(properties.getProperty(BIND_ADDRESS));
        if( uri.getHost().equals("0.0.0.0")) {
            LOG.error("retz.bind is told to Mesos; {}/32 should not be assigned", uri.getHost());
            throw new IllegalArgumentException();
        }
        if (uri.getPort() < 1024 || 65536 < uri.getPort()) {
            LOG.error("retz.bind must not use well known port, or just too large: {}", uri.getPort());
            throw new IllegalArgumentException();
        }

        String gpu = properties.getProperty(USE_GPU, "false");
        if (gpu.equals("true")) {
            useGPU = true;
        } else if (gpu.equals("false")) {
            useGPU = false;
        } else {
            throw new IllegalArgumentException(USE_GPU + "must be boolean");
        }
	
        LOG.info("Mesos master={}, principal={}, role={}", getMesosMaster(), getPrincipal(), getRole());
    }

    public String getMesosMaster() {
        return properties.getProperty(MESOS_LOC_KEY);
    }

    public URI getUri() {
        return uri;
    }

    public String getPrincipal() {
        // Principal is required to reserve volumes
        String principal = properties.getProperty(MESOS_PRINCIPAL, DEFAULT_MESOS_PRINCIPAL);
        if (principal.isEmpty()) {
            return DEFAULT_MESOS_PRINCIPAL;
        }
        return principal;
    }

    public String getRole() {
        // Same role as principal
        return properties.getProperty(MESOS_ROLE, getPrincipal());
    }

    public String getUserName() {
        return properties.getProperty(USER_NAME, System.getProperty("user.name"));
    }

    public boolean useGPU() {
        return useGPU;
    }

    public String getZkServers() {
	return properties.getProperty(ZK_SERVERS);
    }
    
    @Override
    public String toString() {
        return new StringBuffer()
                .append("[uri=").append(uri)
                .append(", props=").append(properties)
                .append("]")
                .toString();
    }
}
