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

    // Authentication enabled by default
    static final String AUTHENTICATION = "retz.authentication";

    // In server, these are for "first" user; while in clients, these are for each user
    static final String ACCESS_KEY = "retz.access.key";
    static final String ACCESS_SECRET = "retz.access.secret";

    // If TLS enabled, these will be used for both server and client
    static final String KEYSTORE_FILE = "retz.tls.keystore.file";
    static final String KEYSTORE_PASS = "retz.tls.keystore.pass";
    static final String TRUSTSTORE_FILE = "retz.tls.truststore.file";
    static final String TRUSTSTORE_PASS = "retz.tls.truststore.pass";
    static final String CHECK_CERT = "retz.tls.insecure";

    protected Properties properties;
    private final boolean authenticationEnabled;
    private final boolean checkCert;


    public FileConfiguration(String path) throws IOException {
        this(new File(path));
    }

    public FileConfiguration(File file) throws IOException {
        this(new FileInputStream(file));
        LOG.debug("Parsed file {}: {}", file.getName(),
                String.join(", ", properties.stringPropertyNames()));
    }

    public FileConfiguration(InputStream in) throws IOException {
        this.properties = new Properties();
        properties.load(in);
        in.close();

        /*
        if (isTLS()) {
            LOG.info("Checking Keystores .. {}", properties.getProperty(KEYSTORE_FILE));
            Objects.requireNonNull(properties.getProperty(KEYSTORE_FILE));
            Objects.requireNonNull(properties.getProperty(KEYSTORE_PASS));
        } */

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

    public static String userAsConfig(User u) {
        StringBuilder builder = new StringBuilder()
                .append(AUTHENTICATION).append(" = true\n")
                .append(ACCESS_KEY).append(" = ").append(u.keyId()).append("\n")
                .append(ACCESS_SECRET).append(" = ").append(u.secret()).append("\n");
        return builder.toString();
    }
}
