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
package io.github.retz.cli;

import io.github.retz.web.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommandPing implements SubCommand {
    static final Logger LOG = LoggerFactory.getLogger(CommandPing.class);

    public final String NAME = "ping";
    public final String DESCRIPTION = "Check server reachability";

    @Override
    public String description() {
        return DESCRIPTION;
    }

    @Override
    public int handle(ClientCLIConfig fileConfig, boolean verbose) throws Throwable {
        if (verbose) {
            LOG.info("Configuration: {}", fileConfig.toString());
        }

        try (Client webClient = Client.newBuilder(fileConfig.getUri())
                .setAuthenticator(fileConfig.getAuthenticator())
                .checkCert(!fileConfig.insecure())
                .setVerboseLog(verbose)
                .build()) {
            if (webClient.ping()) {
                if (verbose) {
                    LOG.info("Successfully reached server {}", fileConfig.getUri());
                }
                return 0;
            } else {
                LOG.error("Cannot reach server {}", fileConfig.getUri());
                return -1;
            }
        }
    }

    @Override
    public String getName() {
        return NAME;
    }
}
