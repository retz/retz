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

import java.util.List;

public class CommandUsage implements SubCommand {
    static final Logger LOG = LoggerFactory.getLogger(CommandUsage.class);

    @Parameter(names = {"--start", "-start"}, description = "Starting time of a period to fetch")
    private String start = "0000-01-01";

    @Parameter(names = {"--end", "-end"}, description = "End time of a period to fetch")
    private String end = "9999-12-32";

    @Override
    public String description() {
        return "Get usage of all users (time range is compared as String)";
    }

    @Override
    public String getName() {
        return "usage";
    }

    @Override
    public int handle(FileConfiguration fileConfig, boolean verbose) throws Throwable {
        int port =fileConfig.getJmxPort();
        try(AdminConsoleClient client = new AdminConsoleClient(new JmxClient("localhost", port))) {
            List<String> lines = client.getUsage(start, end);
            for(String line: lines) {
                LOG.info(line);
            }
            return 0;
        }
    }
}

