/**
 *    Retz
 *    Copyright (C) 2016 Nautilus Technologies, KK.
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
package io.github.retz.mesos;

import io.github.retz.cli.FileConfiguration;
import io.github.retz.web.WebConsole;
import org.apache.commons.cli.*;
import org.apache.mesos.MesosSchedulerDriver;
import org.apache.mesos.Protos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URISyntaxException;

public final class MesosFrameworkLauncher {
    private static final Logger LOG = LoggerFactory.getLogger(MesosFrameworkLauncher.class);

    public static void main(String... argv) {
        System.exit(run(argv));
    }

    public static int run(String... argv) {

        Configuration conf;
        try {
            conf = parseConfiguration(argv);
        } catch (ParseException e) {
            LOG.error(e.getMessage());
            return -1;
        } catch (URISyntaxException e) {
            LOG.error(e.getMessage());
            return -1;
        } catch (IOException e) {
            LOG.error(e.getMessage());
            return -1;
        }

        Protos.FrameworkInfo fw = buildFrameworkInfo(conf);

        RetzScheduler scheduler = new RetzScheduler(conf, fw);
        MesosSchedulerDriver driver = null;
        try {
            driver = new MesosSchedulerDriver(scheduler, fw, conf.getMesosMaster());
        } catch (Exception e) {
            LOG.error("Cannot start Mesos scheduler: {}", e.getMessage());
            return -1;
        }
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
        int port = conf.getPort();
        WebConsole webConsole = new WebConsole(port);
        WebConsole.setScheduler(scheduler);
        WebConsole.setDriver(driver);
        LOG.info("Web console has started with port {}", port);

        // Stop them all
        // Wait for Mesos framework stop
        status = driver.join();
        LOG.info("{} has been stopped: {}", RetzScheduler.FRAMEWORK_NAME, status.name());

        webConsole.stop(); // Stop web server

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
    private static final Options OPTIONS;

    static {
        OPT_CONFIG = new Option("C", "config", true, "Configuration file path");
        OPT_CONFIG.setArgName("/path/to/retz.properties");

        OPTIONS = new Options();
        OPTIONS.addOption(OPT_CONFIG);
    }

    static Configuration parseConfiguration(String[] argv) throws ParseException, IOException, URISyntaxException {
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(OPTIONS, argv); //argumentList.getStandardAsArray());

        // This default path must match the prefix in build.gradle
        String configFile = cmd.getOptionValue(OPT_CONFIG.getOpt(), "/opt/retz-server/etc/retz.properties");

        Configuration conf = new Configuration(new FileConfiguration(configFile));
        LOG.info("Binding as {}", conf.fileConfig.getUri()); // TODO hostname, protocol

        return conf;
    }

    public static final class Configuration {
        FileConfiguration fileConfig;

        public Configuration(FileConfiguration fileConfig) {
            this.fileConfig = fileConfig;
        }

        public int getPort() {
            return fileConfig.getUri().getPort();
        }

        public String getMesosMaster() {
            return fileConfig.getMesosMaster();
        }

    }
}
