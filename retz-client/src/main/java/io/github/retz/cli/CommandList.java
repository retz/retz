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

import io.github.retz.protocol.Job;
import io.github.retz.protocol.ListJobResponse;
import io.github.retz.web.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.ConnectException;
import java.net.URISyntaxException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class CommandList implements SubCommand {
    static final Logger LOG = LoggerFactory.getLogger(CommandList.class);

    public final String NAME = "list";
    public final String DESCRIPTION = "list all jobs";

    @Override
    public String description() {
        return DESCRIPTION;
    }

    @Override
    public int handle(FileConfiguration fileConfig) {
        LOG.debug("Configuration: {}", fileConfig.toString());

        try (Client webClient = new Client(fileConfig.getUri().getHost(),
                fileConfig.getUri().getPort())) {
            webClient.connect();

            ListJobResponse r = (ListJobResponse) webClient.list(64); // TODO: make this CLI argument
            List<Job> jobs = new LinkedList<>();
            jobs.addAll(r.queue());
            jobs.addAll(r.running());
            jobs.addAll(r.finished());

            TableFormatter formatter = new TableFormatter(
                    "TaskId", "State", "AppName", "Command", "Result", "Duration",
                    "Scheduled", "Started", "Finished", "Reason");

            jobs.sort((a, b) -> a.id() - b.id());

            for (Job job : jobs) {
                String reason = "-";
                if (job.reason() != null) {
                    reason = "'" + job.reason() + "'";
                }
                String duration = "-";
                if (job.started() != null && job.finished() != null) {
                    try {
                        duration = Double.toString(TimestampHelper.diffMillisec(job.finished(), job.started()) / 1000.0);
                    } catch (java.text.ParseException e) {
                    }
                }
                formatter.feed(Integer.toString(job.id()), job.state().toString(), job.appid(), job.cmd(), Integer.toString(job.result()), duration,
                        job.scheduled(), job.started(), job.finished(), reason);
            }
            LOG.info(formatter.titles());
            for (String line : formatter) {
                LOG.info(line);
            }
            return 0;
        } catch (URISyntaxException e) {
            LOG.error(e.toString());
        } catch (ConnectException e) {
            LOG.error("Cannot connect to server {}", fileConfig.getUri());
        } catch (ExecutionException e) {
            LOG.error(e.toString());
        } catch (IOException e) {
            LOG.error(e.toString(), e);
        }
        return -1;
    }

    @Override
    public String getName() {
        return NAME;
    }
}
