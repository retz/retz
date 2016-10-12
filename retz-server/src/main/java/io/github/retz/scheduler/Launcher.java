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

import com.google.protobuf.ByteString;
import io.github.retz.cli.FileConfiguration;
import io.github.retz.web.WebConsole;
import org.apache.commons.cli.*;
import org.apache.mesos.MesosSchedulerDriver;
import org.apache.mesos.Protos;
import org.apache.mesos.SchedulerDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Objects;

public final class Launcher {
    private static final Logger LOG = LoggerFactory.getLogger(Launcher.class);

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
            Database.init(conf.getFileConfig());
        } catch (ParseException e) {
            LOG.error(e.toString());
            return -1;
        } catch (URISyntaxException e) {
            LOG.error(e.toString());
            return -1;
        } catch (IOException e) {
            LOG.error(e.toString());
            return -1;
        }

        Protos.FrameworkInfo fw = buildFrameworkInfo(conf);


        RetzScheduler scheduler = new RetzScheduler(conf, fw);
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
        WebConsole webConsole = new WebConsole(conf.fileConfig);
        WebConsole.setScheduler(scheduler);
        WebConsole.setDriver(driver);
        LOG.info("Web console has started with port {}", conf.getPort());

        java.lang.Runtime.getRuntime().addShutdownHook(new ShutdownThread(webConsole, driver));

        // Stop them all, usually don't come here
        // Wait for Mesos framework stop
        status = driver.join();
        LOG.info("{} has been stopped: {}", RetzScheduler.FRAMEWORK_NAME, status.name());

        webConsole.stop(); // Stop web server
        Database.stop();

        return (status == Protos.Status.DRIVER_STOPPED ? 0 : 255);
    }

    private static Protos.FrameworkInfo buildFrameworkInfo(Configuration conf) {
        String userName = conf.fileConfig.getUserName();

        Protos.FrameworkInfo.Builder fwBuilder = Protos.FrameworkInfo.newBuilder()
                .setUser(userName)
                .setName(RetzScheduler.FRAMEWORK_NAME)
                .setWebuiUrl(conf.fileConfig.getUri().toASCIIString())
                .setFailoverTimeout(0)
                .setCheckpoint(true)
                .setPrincipal(conf.fileConfig.getPrincipal())
                .setRole(conf.fileConfig.getRole());

        if (conf.fileConfig.useGPU()) {
            LOG.info("GPU enabled - registering with GPU_RESOURCES capability.");
            fwBuilder.addCapabilities(Protos.FrameworkInfo.Capability.newBuilder()
                    .setType(Protos.FrameworkInfo.Capability.Type.GPU_RESOURCES)
                    .build());
        }

        LOG.info("Connecting to Mesos master {} as {}", conf.getMesosMaster(), userName);
        return fwBuilder.build();
    }

    static final Option OPT_CONFIG;
    static final Option OPT_MODE;
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

    static Configuration parseConfiguration(String[] argv) throws ParseException, IOException, URISyntaxException {
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(OPTIONS, argv); //argumentList.getStandardAsArray());

        // This default path must match the prefix in build.gradle
        String configFile = cmd.getOptionValue(OPT_CONFIG.getOpt(), "/opt/retz-server/etc/retz.properties");

        Configuration conf = new Configuration(new FileConfiguration(configFile));
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
        FileConfiguration fileConfig;

        enum Mode {
            LOCAL,
            MESOS
        }

        Mode launchMode;

        public Configuration(FileConfiguration fileConfig) {
            Objects.requireNonNull(fileConfig, "File configuration cannot be null");
            Objects.requireNonNull(fileConfig.getMesosMaster(), "Mesos master location cannot be empty");

            this.fileConfig = fileConfig;
        }

        public int getPort() {
            return fileConfig.getUri().getPort();
        }

        public String getMesosMaster() {
            return fileConfig.getMesosMaster();
        }

        public FileConfiguration getFileConfig() {
            return fileConfig;
        }

    }
}
