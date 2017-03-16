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

import io.github.retz.cli.FileConfiguration;
import io.github.retz.protocol.data.ResourceQuantity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

public class ServerConfiguration extends FileConfiguration {

    // System Limits
    public static final String MAX_SIMULTANEOUS_JOBS = "retz.max.running";
    public static final String DEFAULT_MAX_SIMULTANEOUS_JOBS = "128";
    public static final String MAX_STOCK_SIZE = "retz.max.stock";
    public static final String DEFAULT_MAX_STOCK_SIZE = "16";
    public static final String MAX_CPUS = "retz.max.cpus";
    public static final String DEFAULT_MAX_CPUS = "8";
    public static final String MAX_MEM = "retz.max.mem";
    public static final String DEFAULT_MAX_MEM = "31744"; // (32 - 1) * 1024 MB
    public static final String MAX_GPUS = "retz.max.gpus";
    public static final String DEFAULT_MAX_GPUS = "0";
    public static final String MAX_PORTS = "retz.max.ports";
    public static final String DEFAULT_MAX_PORTS = "10";
    public static final String MAX_DISK = "retz.max.disk";
    public static final String DEFAULT_MAX_DISK = "1024"; // in MB
    public static final String MAX_LIST_JOB_SIZE = "retz.max.list-jobs";
    public static final String DEFAULT_MAX_LIST_JOB_SIZE = "65536";

    // Mesos connections and so on
    static final String MESOS_LOC_KEY = "retz.mesos";
    // TODO: Sparkjava (http://sparkjava.com) only binds to 0.0.0.0, but it must be any IP address
    static final String BIND_ADDRESS = "retz.bind";
    static final String MESOS_ROLE = "retz.mesos.role";
    static final String MESOS_PRINCIPAL = "retz.mesos.principal";
    static final String DEFAULT_MESOS_PRINCIPAL = "retz";
    static final String MESOS_SECRET_FILE = "retz.mesos.secret.file";
    static final String MESOS_REFUSE_SECONDS = "retz.mesos.refuse";
    static final int DEFAULT_MESOS_REFUSE_SECONDS = 3;

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
    static final String[] INVALID_BIND_ADDRESS = {"0.0.0.0", "localhost", "127.0.0.1"};
    private static final Logger LOG = LoggerFactory.getLogger(ServerConfiguration.class);
    private final URI uri;
    private final int maxSimultaneousJobs;
    private final String databaseURL;
    private final String databaseDriver;

    private final String PLANNER_NAME = "retz.planner.name";
    private final String DEFAULT_PLANNER_NAME = "naive";
    private final String[] PLANNER_NAMES = {"naive", "priority", "fifo", "priority2"};

    private final String ADDITIONAL_CLASSPATH = "retz.classpath";
    private final String DEFAULT_ADDITIONAL_CLASSPATH = "/opt/retz-server/lib";

    // Leeway seconds before old job entries get deleted by Retz.
    // As we may think of many race conditions on task finish at Mesos
    // and retry from Retz, updates, this value must be reasonably
    // large number, like several days that old tasks my not reincarnate.
    // For example, if a cluster usage accounting batch is scheduled
    // every week, this must be long enough that *all* jobs are
    // accounted and not deleted before any batch.
    private final String GC = "retz.gc";
    private final String GC_LEEWAY = "retz.gc.leeway";
    private final int DEFAULT_GC_LEEWAY = 7 * 86400; // a week in seconds
    private final String GC_INTERVAL = "retz.gc.interval";
    private final int DEFAULT_GC_INTERVAL = 600; // 10 minutes in seconds


    public ServerConfiguration(InputStream in) throws IOException, URISyntaxException {
        super(in);

        List<String> invalidBindAddresses = Arrays.asList(INVALID_BIND_ADDRESS);

        Objects.requireNonNull(properties.getProperty(BIND_ADDRESS), "Host and port are required");
        Objects.requireNonNull(getMesosMaster(), "Mesos address must not be empty");

        if (invalidBindAddresses.contains(getMesosMaster().split(":")[0])) {
            LOG.error("{} (now {}) must not be any of {}", MESOS_LOC_KEY, getMesosMaster(),
                    String.join(", ", invalidBindAddresses));
            throw new IllegalArgumentException();
        }

        uri = new URI(properties.getProperty(BIND_ADDRESS));
        // loopback addresses must also be checked, but for now inttest uses localhost address
        if (uri.getHost().equals("0.0.0.0")) {
            LOG.error("retz.bind is told to Mesos; {}/32 should not be assigned", uri.getHost());
            throw new IllegalArgumentException();
        }
        if (uri.getPort() < 1024 || 65536 < uri.getPort()) {
            LOG.error("retz.bind must not use well known port, or just too large: {}", uri.getPort());
            throw new IllegalArgumentException();
        }

        maxSimultaneousJobs = Integer.parseInt(properties.getProperty(MAX_SIMULTANEOUS_JOBS, DEFAULT_MAX_SIMULTANEOUS_JOBS));
        if (maxSimultaneousJobs < 1) {
            throw new IllegalArgumentException(MAX_SIMULTANEOUS_JOBS + " must be positive");
        }

        databaseURL = properties.getProperty(DATABASE_URL, DEFAULT_DATABASE_URL);
        databaseDriver = properties.getProperty(DATABASE_DRIVER_CLASS, DEFAULT_DATABASE_DRIVER_CLASS);

        if ("root".equals(getUserName()) || getUserName().isEmpty()) {
            LOG.error("{} must not be 'root' nor empty", USER_NAME);
            throw new IllegalArgumentException("Invalid parameter: " + USER_NAME);
        }

        if (getRefuseSeconds() < 1) {
            throw new IllegalArgumentException(MESOS_REFUSE_SECONDS + " must be positive integer");
        }

        LOG.info("Mesos master={}, principal={}, role={}, {}={}, {}={}, {}={}, {}={}, {}={}, {}={}, {}={}, {}={}",
                getMesosMaster(), getPrincipal(), getRole(), MAX_SIMULTANEOUS_JOBS, maxSimultaneousJobs,
                DATABASE_URL, databaseURL,
                MAX_STOCK_SIZE, getMaxStockSize(),
                USER_NAME, getUserName(),
                MESOS_REFUSE_SECONDS, getRefuseSeconds(),
                GC_LEEWAY, getGcLeeway(),
                GC_INTERVAL, getGcInterval(),
                MAX_LIST_JOB_SIZE, getMaxJobSize());
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

    public String getSecretFile() {
        return properties.getProperty(MESOS_SECRET_FILE);
    }

    public String getRole() {
        // Same role as principal
        return properties.getProperty(MESOS_ROLE, getPrincipal());
    }

    public String getUserName() {
        return properties.getProperty(USER_NAME, "nobody");
    }

    public boolean useGPU() {
        return Integer.parseInt(properties.getProperty(MAX_GPUS, DEFAULT_MAX_GPUS)) > 0;
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

    public ResourceQuantity getMaxJobSize() {
        return new ResourceQuantity(
                Integer.parseInt(properties.getProperty(MAX_CPUS, DEFAULT_MAX_CPUS)),
                Integer.parseInt(properties.getProperty(MAX_MEM, DEFAULT_MAX_MEM)),
                Integer.parseInt(properties.getProperty(MAX_GPUS, DEFAULT_MAX_GPUS)),
                Integer.parseInt(properties.getProperty(MAX_PORTS, DEFAULT_MAX_PORTS)),
                Integer.parseInt(properties.getProperty(MAX_DISK, DEFAULT_MAX_DISK)),
                0);
    }

    public String getPlannerName() {
        return properties.getProperty(PLANNER_NAME, DEFAULT_PLANNER_NAME);
    }

    public int getRefuseSeconds() {
        return getLowerboundedIntProperty(MESOS_REFUSE_SECONDS, DEFAULT_MESOS_REFUSE_SECONDS, 1);
    }

    public boolean getGc() {
        return getBoolProperty(GC, true);
    }

    public int getGcLeeway() {
        return getLowerboundedIntProperty(GC_LEEWAY, DEFAULT_GC_LEEWAY, 0);
    }

    public int getGcInterval() {
        return getLowerboundedIntProperty(GC_INTERVAL, DEFAULT_GC_INTERVAL, 1);
    }

    public int getMaxListJobSize() {
        return Integer.parseInt(properties.getProperty(MAX_LIST_JOB_SIZE, DEFAULT_MAX_LIST_JOB_SIZE));
    }

    public Properties copyAsProperties() {
        return (Properties)properties.clone();
    }

    public String classpath() {
        return properties.getProperty(ADDITIONAL_CLASSPATH, DEFAULT_ADDITIONAL_CLASSPATH);
    }

    public boolean isBuiltInPlanner() {
        List builtInPlanners = Arrays.asList(PLANNER_NAMES);
        return builtInPlanners.contains(getPlannerName());
    }

    @Override
    public String toString() {
        return new StringBuffer()
                .append("[uri=").append(uri)
                .append(", props=").append(properties)
                .append(", ").append(MAX_STOCK_SIZE).append("=").append(getMaxStockSize())
                .append(", insecure=").append(insecure())
                .append("]")
                .toString();
    }

}
