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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.ObjectName;

public class CommandEnableUser implements SubCommand {
    static final Logger LOG = LoggerFactory.getLogger(CommandEnableUser.class);

    @Parameter(names = "-id", description = "User ID you want to enable", required = true)
    private String id;

    @Override
    public String description() {
        return "Enable a user";
    }

    @Override
    public String getName() {
        return "enable-user";
    }

    @Override
    public int handle(FileConfiguration fileConfig, boolean verbose) throws Throwable {
        int port = fileConfig.getJmxPort();
        try(JmxClient jmxClient = new JmxClient("localhost", port)) {
            Object o = jmxClient.invokeOperation(new ObjectName("io.github.retz.scheduler:type=AdminConsole"), "enableUser", id, true);
            boolean result = (Boolean)o;
            LOG.info("User enabled: {}", result);
            return 0;
        }
    }
}

