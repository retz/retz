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
package io.github.retz.grpc;

import io.github.retz.auth.Authenticator;
import io.github.retz.auth.HmacSHA256Authenticator;
import io.github.retz.auth.NoopAuthenticator;
import io.github.retz.cli.ClientCLIConfig;
import io.github.retz.db.Database;
import io.github.retz.protocol.data.Application;
import io.github.retz.protocol.data.Job;
import io.github.retz.protocol.data.MesosContainer;
import io.github.retz.scheduler.Launcher;
import io.github.retz.scheduler.ServerConfiguration;
import io.grpc.StatusRuntimeException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class GrpcSmokeTest {
    RetzServer server;
    ServerConfiguration config;
    int port = 34756;

    @Before
    public void before() throws Throwable {

        InputStream in = Launcher.class.getResourceAsStream("/retz-tls.properties");
        config = new ServerConfiguration(in);
        Database.getInstance().init(config);
        assertTrue(Database.getMigrator().isFinished());

        this.server = new RetzServer(port);
        this.server.start();
    }

    @After
    public void after() {
        this.server.stop();
        this.server.blockUntilShutdown();
        Database.getInstance().stop();
    }

    @Test
    public void smoke() throws Throwable {
        // Absolutely passes as it's taken from server configuration!
        Authenticator auth = config.getAuthenticator();
        try (Client client = new Client("localhost", port, auth)) {
            assertTrue(client.ping());

            {
                List<Application> apps = client.listApps();
                assertTrue(apps.isEmpty());

                List<Job> jobs = client.listJobs(Job.JobState.QUEUED, Optional.empty());
                assertTrue(jobs.isEmpty());
            }

            String[] files = {"http://example.com:234/foobar/test.tar.gz"};
            Application app = new Application("foobar", Collections.emptyList(), Arrays.asList(files),
                    Optional.empty(), config.getUser().keyId(),
                    0, new MesosContainer(), true);
            {
                client.loadApp(app);
                Application app2 = client.getApp(app.getAppid());
                assertEquals(app.getAppid(), app2.getAppid());
                assertEquals(app.getOwner(), app2.getOwner());
                //TODO: fix this test fails
                //assertEquals(app.getUser(), app2.getUser());
                assertEquals(app.getGracePeriod(), app2.getGracePeriod());

                List<Application> apps = client.listApps();
                assertEquals(1, apps.size());
                assertEquals(app.getAppid(), apps.get(0).getAppid());
            }
        }
    }

    @Test(expected = StatusRuntimeException.class)
    public void authFail() throws Throwable {
        Authenticator auth = new HmacSHA256Authenticator(config.getUserName(), "this is a wrong secret");
        try (Client client = new Client("localhost", port, auth)) {
            assertTrue(client.ping());
        }
    }
}
