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

import com.j256.simplejmx.server.JmxServer;
import io.github.retz.db.Database;
import io.github.retz.jmx.RetzJmxServer;
import io.github.retz.mesosc.MesosHTTPFetcher;
import io.github.retz.misc.LogUtil;
import io.github.retz.protocol.data.Job;
import io.github.retz.web.WebConsole;
import org.apache.commons.cli.*;
import org.apache.mesos.Protos;
import org.apache.mesos.SchedulerDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.util.*;
import java.util.stream.Collectors;

public final class Launcher {
    static final Option OPT_CONFIG;
    static final Option OPT_MODE;
    private static final Logger LOG = LoggerFactory.getLogger(Launcher.class);
    private static final Options OPTIONS;

    static {
        OPT_CONFIG = new Option("C", "config", true, "Configuration file path");
        OPT_CONFIG.setArgName("/path/to/retz.properties");

        OPT_MODE = new Option("M", "mode", true, "Scheduler mode ( local|mesos )");
        OPT_MODE.setArgName("mesos");

        OPTIONS = new Options();
        OPTIONS.addOption(OPT_CONFIG);
        OPTIONS.addOption(OPT_MODE);
    }

    public static void main(String... argv) {
        System.exit(run(argv));
    }

    public static int run(String... argv) {

        Configuration conf;
        try {
            conf = parseConfiguration(argv);
            if (conf.fileConfig.isTLS()) {
                LOG.warn("Make sure a valid certificate is being used or RetzExecutor may not work.");
            }
            Database.getInstance().init(conf.getServerConfig());
            if (conf.getServerConfig().getGc()) {
                GarbageJobCollector.start(conf.getServerConfig().getGcLeeway(), conf.getServerConfig().getGcInterval());
            } else {
                LOG.info("Automatic garbage collection is turned off; use retz-admin gc to collect old jobs");
            }
        } catch (ParseException | URISyntaxException | IOException e) {
            LogUtil.error(LOG, "launch error", e);
            return -1;
        }

        Optional<JmxServer> maybeJmxServer = RetzJmxServer.start(conf.getServerConfig());
        if (!maybeJmxServer.isPresent()) {
            LOG.error("Failed to start JMX Server");
            return -1;
        }

        Protos.FrameworkInfo fw = buildFrameworkInfo(conf);

        // Retz must do all recovery process before launching scheduler;
        // This is because running scheduler changes state of any jobs if it
        // has successfully connected to Mesos master.
        // By hitting HTTP endpoints and comparing with database job states,
        // Retz can decide whether to re-run it or just finish it.
        // BTW after connecting to Mesos it looks like re-sending unacked messages.
        // TODO: this call should be replaced with reconcillation after reconnect / registerred callback
        // maybeRequeueRunningJobs(conf.getMesosMaster(), fw.getId().getValue(), Database.getInstance().getRunning());

        RetzScheduler scheduler;
        try {
            scheduler = new RetzScheduler(conf, fw);
        } catch (Throwable t) {
            LOG.error("Cannot initialize scheduler", t);
            return -1;
        }
        SchedulerDriver driver = SchedulerDriverFactory.create(scheduler, conf, fw);

        Protos.Status status = driver.start();

        if (status != Protos.Status.DRIVER_RUNNING) {
            LOG.error("Cannot start Mesos scheduler: {}", status.name());
            System.exit(-1);
            //} else if (status == Protos.Status.DRIVER_ABORTED) {
            //} else if (status == Protos.Status.DRIVER_NOT_STARTED) {
            //} else if (status == Protos.Status.DRIVER_STOPPED) {
        }

        LOG.info("Mesos scheduler started: {}", status.name());

        // Start web server
        WebConsole.start(conf.fileConfig);
        WebConsole.set(scheduler, driver);
        LOG.info("Web console has started with port {}", conf.getPort());

        java.lang.Runtime.getRuntime().addShutdownHook(new ShutdownThread(driver));

        // Stop them all, usually don't come here
        // Wait for Mesos framework stop
        status = driver.join();
        LOG.info("{} has been stopped: {}", RetzScheduler.FRAMEWORK_NAME, status.name());

        WebConsole.stop(); // Stop web server
        GarbageJobCollector.stop();
        Database.getInstance().stop();
        maybeJmxServer.get().stop();

        return (status == Protos.Status.DRIVER_STOPPED ? 0 : 255);
    }

    private static void maybeRequeueRunningJobs(String master, String frameworkId, List<Job> running) throws IOException {
        LOG.info("{} jobs found in DB 'STARTING' or 'STARTED' state. Requeuing...", running.size());
        int offset = 0;
        int limit = 128;
        Map<String, Job> runningMap = running.stream().collect(Collectors.toMap(job -> job.taskId(), job -> job));
        List<Job> recoveredJobs = new ArrayList<>();
        while (true) {
            try {
                List<Map<String, Object>> tasks = MesosHTTPFetcher.fetchTasks(master, frameworkId, offset, limit);
                if (tasks.isEmpty()) {
                    break;
                }

                for (Map<String, Object> task : tasks) {
                    String state = (String) task.get("state");
                    // Get TaskId
                    String taskId = (String) task.get("id");
                    if (runningMap.containsKey(taskId)) {
                        Job job = runningMap.remove(taskId);
                        recoveredJobs.add(JobQueue.updateJobStatus(job, state));
                    } else {
                        LOG.warn("Unknown job!");
                    }
                }
                offset = offset + tasks.size();
            } catch (MalformedURLException e) {
                LOG.error(e.toString(), e);
                throw new RuntimeException(e);
            }
        }
        Database.getInstance().updateJobs(recoveredJobs);
        LOG.info("{} jobs rescheduled, {} jobs didn't need change.", recoveredJobs.size(), runningMap.size());
    }

    private static Protos.FrameworkInfo buildFrameworkInfo(Configuration conf) {
        String userName = conf.fileConfig.getUserName();

        Protos.FrameworkInfo.Builder fwBuilder = Protos.FrameworkInfo.newBuilder()
                .setUser(userName)
                .setName(RetzScheduler.FRAMEWORK_NAME)
                .setWebuiUrl(conf.fileConfig.getUri().toASCIIString())
                .setFailoverTimeout(conf.getServerConfig().getFailoverTimeout())
                .setCheckpoint(true)
                .setPrincipal(conf.fileConfig.getPrincipal());

        if (conf.getServerConfig().getRole().isPresent()) {
            LOG.info("MULTI_ROLES capability set (role={})", conf.fileConfig.getRole().get());
            fwBuilder.addCapabilities(Protos.FrameworkInfo.Capability.newBuilder()
                    .setType(Protos.FrameworkInfo.Capability.Type.MULTI_ROLE)
                    .build())
                    .addRoles(conf.fileConfig.getRole().get());
        }

        Optional<String> fid;
        try {
            fid = Database.getInstance().getFrameworkId();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        if (fid.isPresent()) {
            LOG.info("FrameworkID {} found", fid.get());
            fwBuilder.setId(Protos.FrameworkID.newBuilder().setValue(fid.get()).build());
        }

        if (conf.fileConfig.useGPU()) {
            LOG.info("GPU enabled - registering with GPU_RESOURCES capability.");
            // TODO: GPU_RESOURCES will be deprecated
            fwBuilder.addCapabilities(Protos.FrameworkInfo.Capability.newBuilder()
                    .setType(Protos.FrameworkInfo.Capability.Type.GPU_RESOURCES)
                    .build());
        }

        LOG.info("Trying to find Mesos master from {} and connecting as {}", conf.getMesosMaster(), userName);
        return fwBuilder.build();
    }

    static Configuration parseConfiguration(String[] argv) throws ParseException, IOException, URISyntaxException {
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(OPTIONS, argv); //argumentList.getStandardAsArray());

        // This default path must match the prefix in build.gradle
        String configFile = cmd.getOptionValue(OPT_CONFIG.getOpt(), "/opt/retz-server/etc/retz.properties");

        Configuration conf = new Configuration(new ServerConfiguration(configFile));
        LOG.info("Binding as {}", conf.fileConfig.getUri()); // TODO hostname, protocol

        String mode = cmd.getOptionValue(OPT_MODE.getOpt(), "mesos");
        if ("local".equals(mode)) {
            conf.launchMode = Configuration.Mode.LOCAL;
            LOG.warn("Using local mode. This is for *TESTS*, don't use this in production");
        } else if ("mesos".equals(mode)) {
            conf.launchMode = Configuration.Mode.MESOS;
        }

        return conf;
    }

    public static final class Configuration {
        ServerConfiguration fileConfig;
        Mode launchMode;

        public Configuration(ServerConfiguration fileConfig) {
            Objects.requireNonNull(fileConfig, "File configuration cannot be null");
            this.fileConfig = fileConfig;
        }

        public int getPort() {
            return fileConfig.getUri().getPort();
        }

        public String getMesosMaster() {
            return fileConfig.getMesosMaster();
        }

        public ServerConfiguration getServerConfig() {
            return fileConfig;
        }

        enum Mode {
            LOCAL,
            MESOS
        }

    }
}
