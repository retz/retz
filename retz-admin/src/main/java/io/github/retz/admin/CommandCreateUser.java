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
import io.github.retz.protocol.data.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommandCreateUser implements SubCommand {
    static final Logger LOG = LoggerFactory.getLogger(CommandCreateUser.class);

    @Parameter(names = "--info", description = "Additional field to remember in database")
    String info = "";

    @Override
    public String description() {
        return "Create a user";
    }

    @Override
    public String getName() {
        return "create-user";
    }

    @Override
    public int handle(FileConfiguration fileConfig, boolean verbose) throws Throwable {
        int port = fileConfig.getJmxPort();

        if (info.length() > 1024) {
            LOG.error("--info must be smaller than 1024 MBs: {}", info);
        }

        try(AdminConsoleClient client = new AdminConsoleClient(new JmxClient("localhost", port))) {
            User u = client.createUserAsObject(info);
            System.out.println(FileConfiguration.userAsConfig(u));
            return 0;
        }
    }
}

