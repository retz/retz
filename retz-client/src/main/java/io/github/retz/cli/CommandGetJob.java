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
import io.github.retz.protocol.GetJobResponse;
import io.github.retz.protocol.Response;
import io.github.retz.protocol.data.Job;
import io.github.retz.web.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.ConnectException;

public class CommandGetJob implements SubCommand {
    static final Logger LOG = LoggerFactory.getLogger(CommandGetJob.class);

    @Parameter(names = "-id", description = "Job ID whose state and details you want", required = true)
    private int id;

    @Override
    public String description() {
        return "Get details of a job";
    }

    @Override
    public String getName() {
        return "get-job";
    }

    @Override
    public int handle(ClientCLIConfig fileConfig, boolean verbose) throws Throwable {
        LOG.debug("Configuration: {}", fileConfig.toString());

        try (Client webClient = Client.newBuilder(fileConfig.getUri())
                .setAuthenticator(fileConfig.getAuthenticator())
                .checkCert(!fileConfig.insecure())
                .setVerboseLog(verbose)
                .build()) {

            if (verbose) {
                LOG.info("Fetching job detail id={}", id);
            }

            Response res = webClient.getJob(id);
            if (res instanceof GetJobResponse) {
                GetJobResponse getJobResponse = (GetJobResponse) res;

                if (getJobResponse.job().isPresent()) {
                    Job job = getJobResponse.job().get();
                    LOG.info(job.pp());
                    return 0;

                } else {
                    LOG.error("No such job: id={}", id);
                }
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

