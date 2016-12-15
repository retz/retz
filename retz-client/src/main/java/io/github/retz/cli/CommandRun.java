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
import io.github.retz.protocol.Response;
import io.github.retz.protocol.ScheduleResponse;
import io.github.retz.protocol.data.Job;
import io.github.retz.web.Client;
import io.github.retz.web.ClientHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;
import java.util.Properties;

public class CommandRun implements SubCommand {
    static final Logger LOG = LoggerFactory.getLogger(CommandRun.class);
    @Parameter(names = {"-E", "--env"},
            description = "Pairs of environment variable names and values, like '-E ASAKUSA_M3BP_OPTS='-Xmx32g' -E SPARK_CMD=path/to/spark-cmd'")
    List<String> envs;
    @Parameter(names = {"--cpu", "-cpu"}, description = "Number of CPU cores assigned to the job")
    int cpu = 1;
    @Parameter(names = {"--mem", "-mem"}, description = "Number of size of RAM(MB) assigned to the job")
    int mem = 32;
    @Parameter(names = {"--gpu", "-gpu"}, description = "Number of GPU cards assigned to the job")
    int gpu = 0;
    @Parameter(names = {"--ports", "-ports"}, description = "Number of ports (up to 1000) required to the job; Ports will be given as $PORT0, $PORT1, ...")
    int ports = 0;
    @Parameter(names = {"--stderr", "-stderr"}, description = "Print stderr after the task finished to standard error")
    boolean stderr = false;
    @Parameter(names = {"-c", "--command", "-cmd"}, required = true, description = "Remote command")
    private String remoteCmd;
    @Parameter(names = {"-A", "--appname"}, required = true, description = "Application name you loaded")
    private String appName;
    @Parameter(names = {"--prio", "--priority"}, description = "Job priority")
    int priority = 0;
    @Parameter(names = {"-N", "--name"}, description = "Human readable job name")
    String name;

    @Override
    public String description() {
        return "Schedule and watch a job";
    }

    @Override
    public String getName() {
        return "run";
    }

    @Override
    public int handle(ClientCLIConfig fileConfig, boolean verbose) throws Throwable {
        Properties envProps = SubCommand.parseKeyValuePairs(envs);

        if (ports < 0 || 1000 < ports) {
            LOG.error("--ports must be within 0 to 1000: {} given.", ports);
            return -1;
        }
        Job job = new Job(appName, remoteCmd, envProps, cpu, mem, gpu, ports);
        job.setPriority(priority);
        job.setName(name);

        try (Client webClient = Client.newBuilder(fileConfig.getUri())
                .setAuthenticator(fileConfig.getAuthenticator())
                .checkCert(!fileConfig.insecure())
                .setVerboseLog(verbose)
                .build()) {

            if (verbose) {
                LOG.info("Sending job {} to App {}", job.cmd(), job.appid());
            }
            Response res = webClient.schedule(job);

            if (!(res instanceof ScheduleResponse)) {
                LOG.error(res.status());
                return -1;
            }
            Job scheduled = ((ScheduleResponse) res).job();
            if (verbose) {
                LOG.info("job {} scheduled", scheduled.id(), scheduled.state());
            }

            Job running = ClientHelper.waitForStart(scheduled, webClient);
            if (verbose) {
                LOG.info("job {} started: {}", running.id(), running.state());
            }

            LOG.info("============ stdout of job {} sandbox start ===========", running.id());
            Optional<Job> finished = ClientHelper.getWholeFile(webClient, running.id(), "stdout", true, System.out);
            LOG.info("============ stdout of job {} sandbox end ===========", running.id());

            if (stderr) {
                LOG.info("============ stderr of job {} sandbox start ===========", running.id());
                Optional<Job> j = ClientHelper.getWholeFile(webClient, running.id(), "stderr", false, System.err);
                LOG.info("============ stderr of job {} sandbox end ===========", running.id());
            }

            if (finished.isPresent()) {
                LOG.debug(finished.get().toString());
                LOG.info("Job(id={}, cmd='{}') finished in {} seconds. status: {}",
                        running.id(), job.cmd(), TimestampHelper.diffMillisec(finished.get().finished(), finished.get().started()) / 1000.0,
                        finished.get().state());
                return finished.get().result();
            } else {
                LOG.error("Failed to fetch last state of job id={}", running.id());
            }
        }
        return -1;
    }
}
