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
import io.github.retz.protocol.Job;
import io.github.retz.web.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.ConnectException;
import java.net.URISyntaxException;
import java.text.ParseException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static io.github.retz.protocol.Range.parseRange;

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

    @Parameter(names = "-gpu", description = "Range of GPU cards assigned to the job in Range")
    private String gpu;

    @Parameter(names = "-trustpvfiles", description = "Whether to trust decompressed files in persistent volume from -P option")
    private boolean trustPVFiles = false;

    @Parameter(names = {"-R", "--resultdir"}, description = "Directory to save job results")
    private String resultDir;

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
                envProps, parseRange(cpu, "1"), parseRange(mem, "32"), parseRange(gpu, "0"));
        job.setTrustPVFiles(trustPVFiles);

        try (Client webClient = new Client(fileConfig.getUri().getHost(),
                fileConfig.getUri().getPort())) {
            webClient.connect();
            LOG.info("Sending job {} to App {}", job.cmd(), job.appid());
            Job result = webClient.run(job);

            if (result != null) {
                Client.fetchJobResult(result, resultDir);
                LOG.info("Job result files URL: {}", result.url());
                LOG.info("Job {} finished in {} seconds and returned {}",
                        job.cmd(), TimestampHelper.diffMillisec(result.finished(), result.started()) / 1000.0,
                        result.result());
                return result.result();
            }

        } catch (ParseException e) {
            LOG.error(e.toString());
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
}
