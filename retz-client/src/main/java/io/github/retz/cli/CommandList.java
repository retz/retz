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
import io.github.retz.protocol.ListJobResponse;
import io.github.retz.protocol.Response;
import io.github.retz.protocol.data.Job;
import io.github.retz.web.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Array;
import java.util.*;
import java.util.stream.Collectors;

public class CommandList implements SubCommand {
    static final Logger LOG = LoggerFactory.getLogger(CommandList.class);

    @Parameter(names = "--state", description = "State of jobs")
    private Job.JobState state;

    @Parameter(names = "--states", description = "States of jobs to list separated by comma, or 'ALL' for all states. Will be ignored when '--state' is used")
    private String states = "QUEUED,STARTING,STARTED";


    @Parameter(names = "--tag", description = "Tag name to show ('AND' condition with states)")
    private String tag;

    public final String NAME = "list";
    public final String DESCRIPTION = "list all jobs";

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

            Optional<String> maybeTag = Optional.ofNullable(tag);
            Set<Job.JobState> jobStates;

            if (state != null) {
                jobStates = new HashSet<>();
                jobStates.add(state);
            } else {
                jobStates = parseStates(states);
            }

            Map<Integer, Job> jobMap = new LinkedHashMap<>();
            for (Job.JobState s : jobStates) {
                if (verbose) {
                    LOG.info("Fetching jobs@{}", s);
                }
                Response res = webClient.list(s, maybeTag); // TODO: make this CLI argument
                if (res instanceof ErrorResponse) {
                    LOG.error(res.status());
                    return -1;
                }
                ListJobResponse r = (ListJobResponse) res;
                for (Job job : r.jobs()) {
                    jobMap.put(job.id(), job);
                }
            }

            TableFormatter formatter = new TableFormatter(
                    "TaskId", "State", "AppName", "Command", "Result", "Duration",
                    "Scheduled", "Started", "Finished", "Tags");

            List<Job> jobs = jobMap.values().stream().sorted(Comparator.comparingInt(job -> job.id())).collect(Collectors.toList());

            for (Job job : jobs) {

                String duration = "-";
                if (job.started() != null && job.finished() != null) {
                    try {
                        duration = Double.toString(TimestampHelper.diffMillisec(job.finished(), job.started()) / 1000.0);
                    } catch (java.text.ParseException e) {
                    }
                }
                String result = "-";
                if (job.state() == Job.JobState.FINISHED ||  job.state() == Job.JobState.KILLED) {
                    result = Integer.toString(job.result());
                }

                if (tag == null || job.tags().contains(tag)) {
                    formatter.feed(Integer.toString(job.id()), job.state().toString(),
                            job.appid(), job.cmd(), result, duration,
                            job.scheduled(), job.started(), job.finished(),
                            String.join(",", job.tags()));
                }
            }
            LOG.info(formatter.titles());
            for (String line : formatter) {
                LOG.info(line);
            }
            return 0;
        }
    }

    @Override
    public String getName() {
        return NAME;
    }

    Set<Job.JobState> parseStates(String states) {

        if ("ALL".equals(states)) {
            return Arrays.stream(Job.JobState.values()).collect(Collectors.toSet());
        }

        Set<Job.JobState> set = new HashSet<>();
        if (states != null) {
            for (String s : states.split(",")) {
                set.add(Job.JobState.valueOf(s));
            }
        }
        return set;
    }
}
