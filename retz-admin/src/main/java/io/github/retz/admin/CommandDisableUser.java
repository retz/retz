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
package io.github.retz.admin;

import com.beust.jcommander.Parameter;
import com.j256.simplejmx.client.JmxClient;
import io.github.retz.cli.FileConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommandDisableUser implements SubCommand {
    static final Logger LOG = LoggerFactory.getLogger(CommandDisableUser.class);

    @Parameter(names = {"--user-id", "-id"}, description = "User ID you want to disable", required = true)
    private String id;

    @Override
    public String description() {
        return "Disable a user";
    }

    @Override
    public String getName() {
        return "disable-user";
    }

    @Override
    public int handle(FileConfiguration fileConfig, boolean verbose) throws Throwable {
        int port = fileConfig.getJmxPort();
        try (AdminConsoleClient client = new AdminConsoleClient(new JmxClient("localhost", port))) {
            boolean result = client.enableUser(id, false);
            LOG.info("User disabled: {}", result);
            return 0;
        }
    }
}

