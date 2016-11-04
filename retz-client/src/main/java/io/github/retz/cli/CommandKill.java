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
import io.github.retz.protocol.KillResponse;
import io.github.retz.protocol.Response;
import io.github.retz.web.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.ConnectException;

public class CommandKill implements SubCommand {
    static final Logger LOG = LoggerFactory.getLogger(CommandKill.class);

    @Parameter(names = "-id", description = "Job ID which you want to kill", required = true)
    private int id;

    @Override
    public String description() {
        return "Kill a job";
    }

    @Override
    public String getName() {
        return "kill";
    }

    @Override
    public int handle(ClientCLIConfig fileConfig) {
        LOG.debug("Configuration: {}", fileConfig.toString());

        try (Client webClient = Client.newBuilder(fileConfig.getUri())
                .enableAuthentication(fileConfig.authenticationEnabled())
                .setAuthenticator(fileConfig.getAuthenticator())
                .checkCert(fileConfig.checkCert())
                .build()) {

            LOG.info("Killing job detail id={}", id);
            Response res = webClient.kill(id);
            if (res instanceof KillResponse) {
                KillResponse killResponse = (KillResponse) res;
                LOG.info(killResponse.status());
                return 0;

            } else {
                ErrorResponse errorResponse = (ErrorResponse) res;
                LOG.error("Error: {}", errorResponse.status());
            }

        } catch (ConnectException e) {
            LOG.error("Cannot connect to server {}", fileConfig.getUri());
        } catch (IOException e) {
            LOG.error(e.toString(), e);
        }
        return -1;

    }
}

