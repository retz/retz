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

import com.beust.jcommander.Parameter;
import io.github.retz.protocol.ErrorResponse;
import io.github.retz.protocol.KillResponse;
import io.github.retz.protocol.ListJobResponse;
import io.github.retz.protocol.Response;
import io.github.retz.protocol.data.Job;
import io.github.retz.web.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

public class CommandKillall implements SubCommand {
    static final Logger LOG = LoggerFactory.getLogger(CommandKillall.class);

    @Parameter(names = {"-t", "--tag"}, description = "Job tag which you want to kill all", required = true)
    private String tag;

    @Override
    public String description() {
        return "Kill a group of jobs";
    }

    @Override
    public String getName() {
        return "killall";
    }

    @Override
    public int handle(ClientCLIConfig fileConfig, boolean verbose) throws Throwable {
        LOG.debug("Configuration: {}", fileConfig.toString());

        try (Client webClient = Client.newBuilder(fileConfig.getUri())
                .setAuthenticator(fileConfig.getAuthenticator())
                .checkCert(!fileConfig.insecure())
                .setVerboseLog(verbose)
                .build()) {

            Response res = webClient.list(64); // TODO: make this CLI argument
            if (res instanceof ErrorResponse) {
                LOG.error(res.status());
                return -1;
            }
            ListJobResponse r = (ListJobResponse) res;

            List<Job> failed = new LinkedList<>();
            List<Job> killed = new LinkedList<>();
            for (Job job : r.queue()) {
                if (job.tags().contains(tag)) {
                    Response response = webClient.kill(job.id());
                    if (response instanceof ErrorResponse) {
                        failed.add(job);
                    } else if (response instanceof KillResponse){
                        killed.add(job);
                    } else {
                        throw new AssertionError(response.getClass().getCanonicalName());
                    }
                }
            }

            for (Job job : r.running()) {
                if (job.tags().contains(tag)) {
                    Response response = webClient.kill(job.id());
                    if (response instanceof ErrorResponse) {
                        failed.add(job);
                    } else if (response instanceof KillResponse){
                        killed.add(job);
                    } else {
                        throw new AssertionError(response.getClass().getCanonicalName());
                    }
                }
            }

            if (verbose) {
                List<String> k = killed.stream().map(job -> Integer.toString(job.id())).collect(Collectors.toList());
                LOG.info("Jobs killed: {}", String.join(", ", k));
            }
            List<String> f = failed.stream().map(job -> Integer.toString(job.id())).collect(Collectors.toList());
            LOG.error("Failed to kill jobs: [{}]", String.join(",", f));

            LOG.info("{} jobs killed, {} jobs failed", killed.size(), failed.size());
            return failed.size();
        }
    }
}

