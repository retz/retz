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

import java.io.IOException;
import java.net.ConnectException;
import java.text.ParseException;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

import static io.github.retz.protocol.data.Range.parseRange;

public class CommandRun implements SubCommand {
    static final Logger LOG = LoggerFactory.getLogger(CommandRun.class);

    @Parameter(names = "-cmd", required = true, description = "Remote command")
    private String remoteCmd;

    @Parameter(names = {"-A", "--appname"}, required = true, description = "Application name you loaded")
    private String appName;

    @Parameter(names = {"-E", "--env"},
            description = "Pairs of environment variable names and values, like '-E ASAKUSA_M3BP_OPTS='-Xmx32g' -E SPARK_CMD=path/to/spark-cmd'")
    List<String> envs;

    @Parameter(names = "-cpu", description = "Range of CPU cores assigned to the job, like '2-'")
    private String cpu;

    @Parameter(names = "-mem", description = "Range of size of RAM(MB) assigned to the job")
    private String mem;

    @Parameter(names = "-gpu", description = "Number of GPU cards assigned to the job in Range")
    private String gpu = "0";

    @Parameter(names = "-trustpvfiles", description = "Whether to trust decompressed files in persistent volume from -P option")
    private boolean trustPVFiles = false;

    @Override
    public String description() {
        return "Schedule and watch a job";
    }

    @Override
    public String getName() {
        return "run";
    }

    @Override
    public int handle(FileConfiguration fileConfig) {
        Properties envProps = SubCommand.parseKeyValuePairs(envs);

        Job job = new Job(appName, remoteCmd,
                envProps, parseRange(cpu, "1"), parseRange(mem, "32"), Integer.parseInt(gpu));
        job.setTrustPVFiles(trustPVFiles);

        try (Client webClient = Client.newBuilder(fileConfig.getUri())
                .enableAuthentication(fileConfig.authenticationEnabled())
                .setAuthenticator(fileConfig.getAuthenticator())
                .checkCert(fileConfig.checkCert())
                .build()) {

            LOG.info("Sending job {} to App {}", job.cmd(), job.appid());
            //Job running = webClient.run(job);
            Response res = webClient.schedule(job);

            if (!(res instanceof ScheduleResponse)) {
                LOG.error(res.status());
                return -1;
            }
            Job scheduled = ((ScheduleResponse) res).job();
            LOG.info("job {} scheduled", scheduled.id(), scheduled.state());

            Job running = ClientHelper.waitForStart(scheduled, webClient);
            LOG.info("job {} started: {}", running.id(), running.state());

            String filename = ClientHelper.maybeGetStdout(running.id(), webClient);

            LOG.info("============ {} in job {} sandbox start ===========", filename, running.id());
            Optional<Job> finished = ClientHelper.getWholeFile(webClient, running.id(), filename, true, System.out);
            LOG.info("============ {} of job {} sandbox end ===========", filename, running.id());
            LOG.info("{} {}", finished.get().state(), finished.get().finished());

            if (finished.isPresent())
            LOG.info("Job(id={}, cmd='{}') finished in {} seconds and returned {}",
                    running.id(), job.cmd(), TimestampHelper.diffMillisec(finished.get().finished(), finished.get().started()) / 1000.0,
                    finished.get().result());
            return running.result();

        } catch (ParseException e) {
            LOG.error(e.toString());
        } catch (ConnectException e) {
            LOG.error("Cannot connect to server {}", fileConfig.getUri());
        } catch (IOException e) {
            LOG.error(e.toString(), e);
        }
        return -1;
    }
}
