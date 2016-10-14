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

import com.beust.jcommander.Parameter;
import io.github.retz.protocol.ErrorResponse;
import io.github.retz.protocol.Response;
import io.github.retz.protocol.data.*;
import io.github.retz.web.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.ConnectException;
import java.util.*;
import java.util.stream.Collectors;

public class CommandLoadApp implements SubCommand {
    static final Logger LOG = LoggerFactory.getLogger(CommandLoadApp.class);

    @Parameter(names = {"-A", "--appname"}, required = true, description = "Application name you loaded")
    private String appName;

    @Parameter(names = {"-F", "--file"}, description = "Smaller files")
    List<String> files = new LinkedList<>();
    //"http://server:8000/path/data.tar.gz,https://server:8000/file2.tar.gz");

    @Parameter(names = {"-L", "--large-file"}, description = "Large files that will be cached at agents")
    List<String> largeFiles = new LinkedList<>();

    @Parameter(names = {"-P", "--persistent"}, description = "Persistent files")
    private List<String> persistentFiles = new LinkedList<>();
    // ("http://server:8000/path/data.tar.gz,https://server:8000/file2.tar.gz");

    @Parameter(names = {"-U", "--user"}, description = "User name to run task")
    private String user;

    @Parameter(names = "-disk", description = "Disk size for persistent volume in MB")
    private int disk = 0;

    @Parameter(names = "--container", description = "Container in which job is run 'mesos' or 'docker'")
    String container = "mesos";

    @Parameter(names = "--image", description = "Container image name to run a job (only available with Docker container)")
    String image;

    @Parameter(names = "--docker-volumes", description = "Docker volume drivers")
    List<String> volumeSpecs = new LinkedList<>();

    @Parameter(names = "--enabled", description = "Enable application [true|false]. If disabled, server returns 401 for any job scheduling request.")
    String enabledStr = "true";

    @Override
    public String description() {
        return "Load an applitation that you want to run";
    }

    @Override
    public String getName() {
        return "load-app";
    }

    @Override
    public int handle(FileConfiguration fileConfig) {
        LOG.debug("Configuration: {}", fileConfig.toString());

        if (files.isEmpty() && persistentFiles.isEmpty()) {
            LOG.warn("No files specified; mesos-execute would rather suite your use case.");
        }
        if (!persistentFiles.isEmpty() && !(disk == 0)) {
            LOG.error("Option '-disk' required when persistent files specified");
            return -1;
        }

        Container c;
        if ("docker".equals(container)) {
            if (image == null) {
                LOG.error("'--image' should not be null when Docker container is selected.");
                return -1;
            }

            List<DockerVolume> dockerVolumes = parseDockerVolumeSpecs(volumeSpecs);
            c = new DockerContainer(image, dockerVolumes);
        } else {
            if ("root".equals(user)) {
                LOG.error("Root user is only allowed at Docker containerizer");
                return -1;
            }
            c = new MesosContainer();
        }

        Optional<Integer> maybeDisk;
        if (disk == 0 || disk < 0) {
            maybeDisk = Optional.empty();
        } else {
            maybeDisk = Optional.of(disk);
        }

        boolean enabled = true;
        if ("false".equals(enabledStr)) {
            enabled = false;
        } else if (! "true".equals(enabledStr)) {
            LOG.error("Wrong argument for --enabled option: {}", enabledStr);
            return -1;
        }
        Application application = new Application(appName,
                persistentFiles, largeFiles, files, maybeDisk,
                Optional.ofNullable(user), fileConfig.getAccessKey(), c,
                enabled);

        try (Client webClient = Client.newBuilder(fileConfig.getUri())
                .enableAuthentication(fileConfig.authenticationEnabled())
                .setAuthenticator(fileConfig.getAuthenticator())
                .checkCert(fileConfig.checkCert())
                .build()) {

            Response r = webClient.load(application);

            LOG.info(r.status());

            if (r instanceof ErrorResponse) {
                return -1;
            }
            return 0;

        } catch (ConnectException e) {
            LOG.error("Cannot connect to server {}", fileConfig.getUri());
        } catch (IOException e) {
            LOG.error(e.toString(), e);
        }
        return -1;
    }

    static List<DockerVolume> parseDockerVolumeSpecs(List<String> volumeSpecs) {
        return volumeSpecs.stream().map(CommandLoadApp::parseVolumeSpec).collect(Collectors.toList());
    }

    static DockerVolume parseVolumeSpec(String spec) {
        List<String> specs = Arrays.asList(spec.split(":"));
        if (specs.size() < 3) {
            throw new IllegalArgumentException("--docker-volumes option had too few arguments: " + specs.size());
        } else if (specs.get(0).isEmpty() || specs.get(1).isEmpty() || specs.get(2).isEmpty()) {
            throw new IllegalArgumentException("--docker-volumes must not have empty element: " + spec);
        }

        String driver = specs.get(0);
        String name = specs.get(1);
        String containerPath = specs.get(2);

        Properties props = parseMoreOptions(specs.subList(3, specs.size()));

        DockerVolume.Mode m = DockerVolume.Mode.RO;
        String mode = props.getProperty("mode", "RO");
        if (!"RO".equals(mode) && !"RW".equals(mode)) {
            LOG.error("Volume mount mode must be either RO or RW");
            throw new IllegalArgumentException();
        }
        if ("RW".equals(mode)) {
            m = DockerVolume.Mode.RW;
        }
        props.remove("mode");

        return new DockerVolume(driver, containerPath, m, name, props);
    }

    static Properties parseMoreOptions(List<String> options) {
        Properties props = new Properties();
        for (String option : options) {
            String[] pair = option.split("=");
            if (pair.length != 2) {
                throw new IllegalArgumentException("Cannot split with '=': " + option);
            }
            props.setProperty(pair[0], pair[1]);
        }
        return props;
    }
}
