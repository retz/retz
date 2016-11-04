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
import io.github.retz.protocol.data.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Objects;
import java.util.Optional;

public class ServerConfiguration extends FileConfiguration {
    private static final Logger LOG = LoggerFactory.getLogger(ServerConfiguration.class);

    static final String MESOS_LOC_KEY = "retz.mesos";
    // TODO: Sparkjava (http://sparkjava.com) only binds to 0.0.0.0, but it must be any IP address
    static final String BIND_ADDRESS = "retz.bind";
    static final String MESOS_ROLE = "retz.mesos.role";

    static final String MESOS_PRINCIPAL = "retz.mesos.principal";
    static final String DEFAULT_MESOS_PRINCIPAL = "retz";

    static final String MESOS_SECRET_FILE = "retz.mesos.secret.file";
    static final String USE_GPU = "retz.gpu";

    // System Limits
    public static final String MAX_SIMULTANEOUS_JOBS = "retz.max.running";
    static final String DEFAULT_MAX_SIMULTANEOUS_JOBS = "128";
    static final String MAX_STOCK_SIZE = "retz.max.stock";
    static final String DEFAULT_MAX_STOCK_SIZE = "16";
    // Not yet used
    static final String QUEUE_MAX = "retz.max.queue";
    static final String SCHEDULE_RESULTS = "retz.results";
    static final String SCHEDULE_RETRY = "retz.retry";

    // Persistence
    static final String DATABASE_URL = "retz.database.url";
    static final String DEFAULT_DATABASE_URL = "jdbc:h2:mem:retz-server;DB_CLOSE_DELAY=-1";
    static final String DATABASE_DRIVER_CLASS = "retz.database.driver";
    static final String DEFAULT_DATABASE_DRIVER_CLASS = "org.h2.Driver";
    static final String DATABASE_USERNAME = "retz.database.user";
    static final String DATABASE_PASSWORD = "retz.database.pass";

    // https://github.com/apache/mesos/blob/master/include/mesos/mesos.proto#L208-L210
    static final String USER_NAME = "retz.user";

    private final URI uri;
    private final int maxSimultaneousJobs;
    private final String databaseURL;
    private final String databaseDriver;
    private final boolean useGPU;

    public ServerConfiguration(InputStream in) throws IOException, URISyntaxException {
        super(in);

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

        maxSimultaneousJobs = Integer.parseInt(properties.getProperty(MAX_SIMULTANEOUS_JOBS, DEFAULT_MAX_SIMULTANEOUS_JOBS));
        if(maxSimultaneousJobs < 1) {
            throw new IllegalArgumentException(MAX_SIMULTANEOUS_JOBS + " must be positive");
        }

        databaseURL = properties.getProperty(DATABASE_URL, DEFAULT_DATABASE_URL);
        databaseDriver = properties.getProperty(DATABASE_DRIVER_CLASS, DEFAULT_DATABASE_DRIVER_CLASS);

        LOG.info("Mesos master={}, principal={}, role={}, {}={}, {}={}, {}={}",
                getMesosMaster(), getPrincipal(), getRole(), MAX_SIMULTANEOUS_JOBS, maxSimultaneousJobs,
                DATABASE_URL, databaseURL,
                MAX_STOCK_SIZE, getMaxStockSize());

    }

    public ServerConfiguration(String file) throws IOException, URISyntaxException {
        this(new FileInputStream(file));

    }

    public String getMesosMaster() {
        return properties.getProperty(MESOS_LOC_KEY);
    }

    public URI getUri() {
        return uri;
    }

    public boolean isTLS() {
        return uri.getScheme().equals("https");
    }

    public String getPrincipal() {
        // Principal is required to reserve volumes
        String principal = properties.getProperty(MESOS_PRINCIPAL, DEFAULT_MESOS_PRINCIPAL);
        if (principal.isEmpty()) {
            return DEFAULT_MESOS_PRINCIPAL;
        }
        return principal;
    }
    public boolean hasSecretFile() {
        return getSecretFile() != null;
    }
    public String getSecretFile()  {
        return properties.getProperty(MESOS_SECRET_FILE);
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

    public int getMaxSimultaneousJobs() {
        return maxSimultaneousJobs;
    }

    public int getMaxStockSize() {
        return Integer.parseInt(properties.getProperty(MAX_STOCK_SIZE, DEFAULT_MAX_STOCK_SIZE));
    }

    public String getDatabaseURL() {
        return databaseURL;
    }

    public String getDatabaseDriver() {
        return databaseDriver;
    }

    public Optional<String> getDatabaseUser() {
        return Optional.ofNullable(properties.getProperty(DATABASE_USERNAME));
    }

    public Optional<String> getDatabasePass() {
        return Optional.ofNullable(properties.getProperty(DATABASE_PASSWORD));
    }

    @Override
    public String toString() {
        return new StringBuffer()
                .append("[uri=").append(uri)
                .append(", props=").append(properties)
                .append(", ").append(MAX_STOCK_SIZE).append("=").append(getMaxStockSize())
                .append(", checkCert=").append(checkCert())
                .append("]")
                .toString();
    }

}
