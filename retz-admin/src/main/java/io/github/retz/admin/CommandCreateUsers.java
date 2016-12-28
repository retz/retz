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
package io.github.retz.admin;

import com.beust.jcommander.Parameter;
import com.j256.simplejmx.client.JmxClient;
import io.github.retz.cli.FileConfiguration;
import io.github.retz.protocol.data.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Paths;
import java.util.stream.Stream;

public class CommandCreateUsers implements SubCommand {
    static final Logger LOG = LoggerFactory.getLogger(CommandCreateUsers.class);

    @Parameter(names = "--file", description = "Additional field to remember in database", required = true)
    String file;

    @Parameter(names = "--output", description = "output directory to create user configuration files")
    String dir = ".";

    @Override
    public String description() {
        return "Create users from a list";
    }

    @Override
    public String getName() {
        return "create-users";
    }

    @Override
    public int handle(FileConfiguration fileConfig, boolean verbose) throws Throwable {
        int port = fileConfig.getJmxPort();

        LOG.info("Reading a list of users. from a file {}", file);

        try (Stream<String> stream = Files.lines(Paths.get(file));
             AdminConsoleClient client = new AdminConsoleClient(new JmxClient("localhost", port))) {
            stream.forEach(line -> {
                if (line.contains(" ")) {
                    LOG.error("Invalid user name: '{}'", line);
                    return;
                }
                try {
                    User u = client.createUserAsObject(line);
                    String config = FileConfiguration.userAsConfig(u);
                    String filename = dir + "/retz.properties-" + line;
                    try(PrintWriter writer = new PrintWriter(filename)) {
                        writer.write(config);
                        LOG.info("Created {}", filename);
                    }

                } catch (IOException e) {
                    LOG.error(e.toString(), e);
                }
            });
            return 0;
        } catch (NoSuchFileException e) {
            LOG.error("No such file");
        }
        return -1;

    }
}

