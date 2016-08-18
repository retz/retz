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
import io.github.retz.protocol.LoadAppResponse;
import io.github.retz.web.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.ConnectException;
import java.net.URISyntaxException;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import static java.util.Optional.ofNullable;

public class CommandLoadApp implements SubCommand {
    static final Logger LOG = LoggerFactory.getLogger(CommandLoadApp.class);

    @Parameter(names = {"-A", "--appname"}, required = true, description = "Application name you loaded")
    private String appName;

    @Parameter(names = {"-F", "--file"}, description = "Asakusa Application environment files")
    List<String> files = new LinkedList<>();
    //"http://server:8000/path/data.tar.gz,https://server:8000/file2.tar.gz");

    @Parameter(names = {"-P","--persistent"}, description = "Asakusa Application environment persistent files")
    private List<String> persistentFiles = new LinkedList<>();
     // ("http://server:8000/path/data.tar.gz,https://server:8000/file2.tar.gz");

    @Parameter(names = "-disk", description = "Disk size for persistent volume in MB")
    private int disk = 0;

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

        try (Client webClient = new Client(fileConfig.getUri().getHost(),
                fileConfig.getUri().getPort())) {
            webClient.connect();

            Optional<Integer> maybeDisk;
            if (disk == 0 || disk < 0) {
                maybeDisk = Optional.empty();
            } else {
                maybeDisk = Optional.of(disk);
            }

            LoadAppResponse r = (LoadAppResponse) webClient.load(appName,
                    persistentFiles, files, maybeDisk);
            LOG.info(r.status());
            return 0;
        } catch (URISyntaxException e) {
            LOG.error(e.getMessage());
        } catch (ConnectException e) {
            LOG.error("Cannot connect to server {}", fileConfig.getUri());
        } catch (ExecutionException e) {
            LOG.error(e.getMessage());
        } catch (IOException e) {
            e.printStackTrace();
        }
        return -1;
    }
}
