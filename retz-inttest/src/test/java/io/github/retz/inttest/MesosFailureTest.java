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
import io.github.retz.protocol.Response;
import io.github.retz.protocol.data.Application;
import io.github.retz.protocol.data.Job;
import io.github.retz.protocol.data.MesosContainer;
import io.github.retz.web.Client;
import org.junit.BeforeClass;
import org.junit.Test;

import java.net.URI;
import java.util.*;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;

/**
 * Created by kuenishi on 17/07/12.
 */
public class MesosFailureTest extends IntTestBase {
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
    public void startAndStop() throws Exception {
        URI uri = new URI("http://" + RETZ_HOST + ":" + RETZ_PORT);

        final String appName = "mesos-failure-test";
        final String[] stop = {"systemctl", "stop", "mesos-master"};
        final String[] start = {"systemctl", "start", "mesos-master"};

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
        }
    }
}
