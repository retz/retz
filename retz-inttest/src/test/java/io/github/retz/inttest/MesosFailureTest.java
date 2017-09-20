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
package io.github.retz.inttest;

import io.github.retz.cli.ClientCLIConfig;
import io.github.retz.protocol.GetFileResponse;
import io.github.retz.protocol.GetJobResponse;
import io.github.retz.protocol.Response;
import io.github.retz.protocol.ScheduleResponse;
import io.github.retz.protocol.data.Application;
import io.github.retz.protocol.data.Job;
import io.github.retz.protocol.data.MesosContainer;
import io.github.retz.web.Client;
import org.apache.commons.io.input.Tailer;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.net.URI;
import java.util.*;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;

public class MesosFailureTest extends IntTestBase {
    private String[] basicStoppers = {"ERROR"};
    private ErrorTrapper errorTrapper = new ErrorTrapper(basicStoppers);

    @BeforeClass
    public static void setupContainer() throws Exception {
        // Starting containers right here as configurations are static
        setupContainer("retz-zk.properties", false);
    }

    @Override
    ClientCLIConfig makeClientConfig() throws Exception {
        return new ClientCLIConfig("src/test/resources/retz-c.properties");
    }

    @Test
    public void syncStartAndStopMaster() throws Exception {
        final String appName = "mesos-failure-test-master";
        syncStartAndStop(appName, "mesos-master");
    }

    @Test
    public void syncStartAndStopAgent() throws Exception {
        final String appName = "mesos-failure-test-agent";
        syncStartAndStop(appName, "mesos-slave");
    }

    @Test
    public void asyncStartAndStopMaster() throws Exception {
        final String appName = "mesos-failure-test-async-master";
        asyncStartAndStop(appName, "mesos-master");
    }

    @Test
    public void asyncStartAndStopAgent() throws Exception {
        final String appName = "mesos-failure-test-async-agent";
        asyncStartAndStop(appName, "mesos-slave");
    }

    private void syncStartAndStop(String appName, String serviceName) throws Exception {
        Tailer tailer = new Tailer(new File("build/log/retz-server.log"), errorTrapper);
        Thread thread = new Thread(tailer);
        thread.start();

        URI uri = new URI("http://" + RETZ_HOST + ":" + RETZ_PORT);

        final String[] stop = {"systemctl", "stop", serviceName};
        final String[] start = {"systemctl", "start", serviceName};

        try (Client client = Client.newBuilder(uri)
                .setAuthenticator(config.getAuthenticator())
                .build()) {
            final Application app = new Application(appName, Collections.emptyList(),
                    Collections.emptyList(), Optional.empty(),
                    "deadbeef", 0, new MesosContainer(), true);
            {
                Response res = client.load(app);
                assertEquals("ok", res.status());
            }
            {
                Job initJob = new Job(appName, "ls", new Properties(), 1, 32, 0);
                Job job = client.run(initJob);
                assertNotNull(job);
                assertEquals(Job.JobState.FINISHED, job.state());
                GetFileResponse res = (GetFileResponse)client.getFile(job.id(), "stdout", 0, -1);
                assertFalse(res.file().get().data().isEmpty());
            }

            // Imitate failure with systemd start/stop - which may lead to graceful stop to schedulers,
            // but until Mesos 1.3.0 Mesos master silently goes away so far.
            container.system(stop);
            container.system(start);

            {
                Job initJob = new Job(appName, "ls", new Properties(), 1, 32, 0);
                Job job = client.run(initJob);
                assertNotNull(job);
                assertEquals(Job.JobState.FINISHED, job.state());
                client.getFile(job.id(), "stdout", 0, -1);
                GetFileResponse res = (GetFileResponse)client.getFile(job.id(), "stdout", 0, -1);
                assertFalse(res.file().get().data().isEmpty());
            }
        } finally {
            tailer.stop();
            for(String line: errorTrapper.getErrors()) {
                System.err.println(line);
            }
            assertFalse("Retz should not write ERROR log:", errorTrapper.getFail());
        }
    }

    private void asyncStartAndStop(String appName, String serviceName) throws Exception {
        Tailer tailer = new Tailer(new File("build/log/retz-server.log"), errorTrapper);
        Thread thread = new Thread(tailer);
        thread.start();

        URI uri = new URI("http://" + RETZ_HOST + ":" + RETZ_PORT);

        final String[] stop = {"systemctl", "stop", serviceName};
        final String[] start = {"systemctl", "start", serviceName};

        try (Client client = Client.newBuilder(uri)
                .setAuthenticator(config.getAuthenticator())
                .build()) {
            final Application app = new Application(appName, Collections.emptyList(),
                    Collections.emptyList(), Optional.empty(),
                    "deadbeef", 0, new MesosContainer(), true);
            {
                Response res = client.load(app);
                assertEquals("ok", res.status());
            }
            Job asyncJob = null;

            // This sleep time must be artibralily long enough that covers following stop-start sequence
            // to make sure reconciliation runs after restart.
            Job initJob1 = new Job(appName, "sleep 32; ls", new Properties(), 1, 32, 0);
            ScheduleResponse response = (ScheduleResponse)client.schedule(initJob1);
            assertNotNull(response.job());
            asyncJob = response.job();

            do {
                GetJobResponse response1 = (GetJobResponse)client.getJob(asyncJob.id());
                assertTrue(response1.job().isPresent());
                asyncJob = response1.job().get();
                Thread.sleep(512);
            } while (asyncJob.state() == Job.JobState.QUEUED);

            // Imitate failure with systemd start/stop - which may lead to graceful stop to schedulers,
            // but until Mesos 1.3.0 Mesos master silently goes away so far.
            container.system(stop);
            Thread.sleep(2048);
            container.system(start);

            do {
                GetJobResponse response1 = (GetJobResponse)client.getJob(asyncJob.id());
                assertTrue(response1.job().isPresent());
                asyncJob = response1.job().get();
                Thread.sleep(2048);
            } while (asyncJob.state() != Job.JobState.FINISHED);

            {
                GetFileResponse response1 = (GetFileResponse)client.getFile(asyncJob.id(), "stdout", 0, -1);
                System.err.println("stdout>" + response1.file().get().data());
                assertFalse(response1.file().get().data().isEmpty());
            }

            {
                Job initJob = new Job(appName, "ls", new Properties(), 1, 32, 0);
                Job job = client.run(initJob);
                assertNotNull(job);
                assertEquals(Job.JobState.FINISHED, job.state());
                client.getFile(job.id(), "stdout", 0, -1);
                GetFileResponse res = (GetFileResponse)client.getFile(job.id(), "stdout", 0, -1);
                assertFalse(res.file().get().data().isEmpty());
            }
        } finally {
            tailer.stop();
            for(String line: errorTrapper.getErrors()) {
                System.err.println(line);
            }
            assertFalse("Retz should not write ERROR log:", errorTrapper.getFail());
        }
    }
}
