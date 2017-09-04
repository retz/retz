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
import io.github.retz.protocol.Response;
import io.github.retz.protocol.ScheduleResponse;
import io.github.retz.protocol.data.Job;
import io.github.retz.web.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class CommandSchedule implements SubCommand {
    static final Logger LOG = LoggerFactory.getLogger(CommandSchedule.class);

    @Parameter(names = {"-E", "--env"},
            description = "Pairs of environment variable names and values, like '-E ASAKUSA_M3BP_OPTS='-Xmx32g' -E SPARK_CMD=path/to/spark-cmd'")
    List<String> envs;
    @Parameter(names = {"--cpu", "-cpu"}, description = "Number of CPU cores assigned to the job")
    int cpu = 1;
    @Parameter(names = {"--mem", "-mem"}, description = "Number of size of RAM(MB) assigned to the job")
    int mem = 32;
    @Parameter(names = {"--gpu", "-gpu"}, description = "Number of GPU cards assigned to the job")
    int gpu = 0;
    @Parameter(names = "--disk", description = "Amount of temporary disk space in MB which the job is going to use")
    int disk = 32;
    @Parameter(names = {"--ports", "-ports"}, description = "Number of ports (up to 1000) required to the job; Ports will be given as $PORT0, $PORT1, ...")
    int ports = 0;
    @Parameter(names = {"-c", "--command", "-cmd"}, required = true, description = "Remote command")
    private String remoteCmd;
    @Parameter(names = {"-A", "--appname"}, required = true, description = "Application name you loaded")
    private String appName;
    @Parameter(names = {"--prio", "--priority"}, description = "Job priority")
    int priority = 0;
    @Parameter(names = {"--attributes"}, description = "Attributes of the job (free format, depends on the planner)")
    String attributes = null;
    @Parameter(names = {"-N", "--name"}, description = "Human readable job name")
    String name;
    @Parameter(names="--tags", description = "Tags separated by commas (e.g. 'a,b,c')")
    List<String> tags = Collections.emptyList();

    @Override
    public String getName() {
        return "schedule";
    }

    @Override
    public String description() {
        return "Schedule a job";
    }

    @Override
    public int handle(ClientCLIConfig fileConfig, boolean verbose) throws IOException {
        Properties envProps = SubCommand.parseKeyValuePairs(envs);

        Job job = new Job(appName, remoteCmd, envProps, cpu, mem, disk, gpu, ports);
        job.setPriority(priority);
        job.setName(name);
        job.addTags(tags);
        job.setAttributes(attributes);

        try (Client webClient = Client.newBuilder(fileConfig.getUri())
                .setAuthenticator(fileConfig.getAuthenticator())
                .checkCert(!fileConfig.insecure())
                .setVerboseLog(verbose)
                .build()) {

            if (verbose) {
                LOG.info("Sending job {} to App {}", job.cmd(), job.appid());
            }
            Response res = webClient.schedule(job);
            if (res instanceof ScheduleResponse) {
                ScheduleResponse res1 = (ScheduleResponse) res;
                LOG.info("Job (id={}): {} registered at {}", res1.job().id(), res1.status(), res1.job.scheduled());
                return 0;
            } else {
                LOG.error("Error: " + res.status());
            }
        }
        return -1;
    }
}

