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
import io.github.retz.protocol.data.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.StringJoiner;

// TODO: Most of items here have become server-specific. Move them out of common to server with proper abstraction
public class FileConfiguration {
    private static final Logger LOG = LoggerFactory.getLogger(FileConfiguration.class);

    static final String MESOS_LOC_KEY = "retz.mesos";
    static final String BIND_ADDRESS = "retz.bind";
    static final String MESOS_ROLE = "retz.mesos.role";

    static final String MESOS_PRINCIPAL = "retz.mesos.principal";
    static final String DEFAULT_MESOS_PRINCIPAL = "retz";

    static final String MESOS_SECRET_FILE = "retz.mesos.secret.file";
    static final String USE_GPU = "retz.gpu";

    // Authentication enabled by default
    static final String AUTHENTICATION = "retz.authentication";

    // In server, these are for admins; while in clients, these are for each user
    static final String ACCESS_KEY = "retz.access.key";
    static final String ACCESS_SECRET = "retz.access.secret";

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

    // If BIND_ADDRESS is for SSL, these will be used for both server and client
    static final String KEYSTORE_FILE = "retz.tls.keystore.file";
    static final String KEYSTORE_PASS = "retz.tls.keystore.pass";
    static final String TRUSTSTORE_FILE = "retz.tls.truststore.file";
    static final String TRUSTSTORE_PASS = "retz.tls.truststore.pass";
    static final String CHECK_CERT = "retz.tls.insecure";

    // https://github.com/apache/mesos/blob/master/include/mesos/mesos.proto#L208-L210
    static final String USER_NAME = "retz.user";

    static final String MESOS_AGENT_JAVA = "retz.executor.java";

    private final Properties properties;
    private final URI uri;
    private final int maxSimultaneousJobs;
    private final String mesosAgentJava;
    private final String databaseURL;
    private final String databaseDriver;
    private final boolean useGPU;
    private final boolean authenticationEnabled;
    private final boolean checkCert;

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
        if (isTLS()) {
            LOG.info("Checking Keystores .. {}", properties.getProperty(KEYSTORE_FILE));
            Objects.requireNonNull(properties.getProperty(KEYSTORE_FILE));
            Objects.requireNonNull(properties.getProperty(KEYSTORE_PASS));
        }

        String gpu = properties.getProperty(USE_GPU, "false");
        if (gpu.equals("true")) {
            useGPU = true;
        } else if (gpu.equals("false")) {
            useGPU = false;
        } else {
            throw new IllegalArgumentException(USE_GPU + "must be boolean");
        }

        String authentication = properties.getProperty(AUTHENTICATION, "true");
        authenticationEnabled = ! authentication.equals("false");
        if (authenticationEnabled) {
            LOG.info("Authentication enabled");
            if (properties.getProperty(ACCESS_SECRET) == null
                    || properties.getProperty(ACCESS_KEY) == null) {
                LOG.error("Both {} and {} should be present in your configuration file",
                        ACCESS_KEY, ACCESS_SECRET);
                throw new IllegalArgumentException("Authentication info lacking");
            }
        } else {
            LOG.warn("Authentication is disabled");
        }

        // Flag name is 'insecure' but this must be treated as right name
        String insecure = properties.getProperty(CHECK_CERT, "false");
        if (insecure.equals("true")) {
            this.checkCert = false;
        } else {
            this.checkCert = true;
        }

        maxSimultaneousJobs = Integer.parseInt(properties.getProperty(MAX_SIMULTANEOUS_JOBS, DEFAULT_MAX_SIMULTANEOUS_JOBS));
        if(maxSimultaneousJobs < 1) {
            throw new IllegalArgumentException(MAX_SIMULTANEOUS_JOBS + " must be positive");
        }

        mesosAgentJava = properties.getProperty(MESOS_AGENT_JAVA, "java");

        databaseURL = properties.getProperty(DATABASE_URL, DEFAULT_DATABASE_URL);
        databaseDriver = properties.getProperty(DATABASE_DRIVER_CLASS, DEFAULT_DATABASE_DRIVER_CLASS);

        LOG.info("Mesos master={}, principal={}, role={}, {}={}, {}={}, {}={}, {}={}",
                getMesosMaster(), getPrincipal(), getRole(), MAX_SIMULTANEOUS_JOBS, maxSimultaneousJobs,
                MESOS_AGENT_JAVA, mesosAgentJava,
                DATABASE_URL, databaseURL,
                MAX_STOCK_SIZE, getMaxStockSize());
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
    public boolean checkCert() {
        return checkCert;
    }
    public String getKeystoreFile() {
        return properties.getProperty(KEYSTORE_FILE);
    }
    public String getKeystorePass() {
        return properties.getProperty(KEYSTORE_PASS);
    }
    public String getTruststoreFile() {
        return properties.getProperty(TRUSTSTORE_FILE);
    }
    public String getTruststorePass() {
        return properties.getProperty(TRUSTSTORE_PASS);
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

    public boolean authenticationEnabled() {
        return authenticationEnabled;
    }

    public String getAccessKey() {
        return properties.getProperty(ACCESS_KEY, "");
    }

    public User getUser() {
        String key = properties.getProperty(ACCESS_KEY);
        String secret = properties.getProperty(ACCESS_SECRET);
        boolean enabled = true;
        if (key == null) {
            throw new RuntimeException("Key cannot be null...."); // TODO: authentication must be always enabled
        }
        if (secret == null) {
            enabled = false;
            secret = "";
        }
        return new User(key, secret, enabled);
    }

    public Authenticator getAuthenticator() {
        String key = properties.getProperty(ACCESS_KEY);
        String secret = properties.getProperty(ACCESS_SECRET);
        if (key == null || secret == null) {
            return null;
        }
        return new Authenticator(key, secret);
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

    public String getMesosAgentJava() {
        return mesosAgentJava;
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
                .append(", checkCert=").append(checkCert)
                .append("]")
                .toString();
    }

    public static String userAsConfig(User u) {
        StringBuilder builder = new StringBuilder()
                .append(AUTHENTICATION).append(" = true")
        .append(ACCESS_KEY).append(" = ").append(u.keyId())
        .append(ACCESS_SECRET).append(" = ").append(u.secret());
        return builder.toString();
    }
}
