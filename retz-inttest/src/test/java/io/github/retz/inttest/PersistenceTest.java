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
package io.github.retz.inttest;

import io.github.retz.cli.ClientCLIConfig;
import io.github.retz.cli.FileConfiguration;
import io.github.retz.protocol.*;
import io.github.retz.protocol.data.Application;
import io.github.retz.protocol.data.Job;
import io.github.retz.protocol.data.MesosContainer;
import io.github.retz.protocol.data.User;
import io.github.retz.web.Client;
import org.hamcrest.Matchers;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.net.URI;
import java.util.*;

import static io.github.retz.inttest.IntTestBase.*;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;


public class PersistenceTest {
    private static final int JOB_AMOUNT = 64;
    private static ClosableContainer container;
    protected ClientCLIConfig config;

    @BeforeClass
    public static void setupContainer() throws Exception {
        container = createContainer(IntTestBase.CONTAINER_NAME);
        container.setConfigfile("retz-persistent.properties");
        container.start();

        System.out.println();
        System.out.println("====================");
        System.out.println("Processes (by ps -awxx)");
        System.out.println(container.ps());
        System.out.println();
        System.out.println("====================");
        System.out.println(container.getRetzServerPid());
    }

    @AfterClass
    public static void cleanupContainer() throws Exception {
        container.close();
    }

    @Before
    public void loadConfig() throws Exception {
        // Mostly same as RetzIntTest
        config = new ClientCLIConfig("src/test/resources/retz-c.properties");
        assertEquals(RETZ_HOST, config.getUri().getHost());
        assertEquals(RETZ_PORT, config.getUri().getPort());
    }

    @Test
    public void persistence() throws Exception {
        User user = config.getUser();
        List<String> e = Arrays.asList();
        Application application = new Application("t", e, e, e, Optional.empty(), Optional.empty(),
                user.keyId(), new MesosContainer(), true);
        URI uri = new URI("http://" + RETZ_HOST + ":" + RETZ_PORT);

        try (Client client = Client.newBuilder(uri).enableAuthentication(true).setAuthenticator(config.getAuthenticator()).build()) {

            client.load(application);

            {
                Response response = client.listApp();
                ListAppResponse listAppResponse = (ListAppResponse) response;
                for (Application a : listAppResponse.applicationList()) {
                    System.err.println(a.getAppid());
                }
            }
            System.err.println("><");

            List<Job> jobs = new LinkedList<>();
            for (int i = 0; i < JOB_AMOUNT; i++) {
                Job job = new Job("t", "echo " + i, new Properties(), 1, 64);
                Response response = client.schedule(job);
                assertThat(response.status(), is("ok"));
                ScheduleResponse scheduleResponse = (ScheduleResponse) response;
                jobs.add(scheduleResponse.job());
            }
            {
                Response response = client.list(JOB_AMOUNT);
                ListJobResponse listJobResponse = (ListJobResponse) response;
                assertThat(listJobResponse.finished().size() + listJobResponse.queue().size() + listJobResponse.running().size(), is(JOB_AMOUNT));
            }

            boolean killed = container.killRetzServerProcess();
            assertTrue(killed);

            container.startRetzServer("retz-persistent.properties");
            container.waitForRetzServer();

            checkApp(uri, application, jobs);
        }
    }

    void checkApp(URI uri, Application application, List<Job> jobs) throws Exception {
        try (Client client = Client.newBuilder(uri).enableAuthentication(true).setAuthenticator(config.getAuthenticator()).build()) {
            {
                Response response = client.listApp();
                ListAppResponse listAppResponse = (ListAppResponse) response;
                assertThat(listAppResponse.applicationList().size(), Matchers.greaterThanOrEqualTo(1));
                for (Application a : listAppResponse.applicationList()) {
                    System.err.println(a.getAppid());
                }
            }
            System.err.println("><");
            {
                Response response = client.getApp(application.getAppid());
                System.err.println(response.status());
                assertThat(response, Matchers.instanceOf(GetAppResponse.class));

                GetAppResponse getAppResponse = (GetAppResponse) response;
                assertThat(getAppResponse.application().getOwner(), is(application.getOwner()));
            }
            System.err.println(">< >< ><");
            {
                Response response = client.list(JOB_AMOUNT);
                ListJobResponse listJobResponse = (ListJobResponse) response;
                assertThat(listJobResponse.finished().size() + listJobResponse.queue().size() + listJobResponse.running().size(), is(JOB_AMOUNT));
            }
            for (int i = 0; i < JOB_AMOUNT / 2; i++) {
                Thread.sleep(4 * 1024);
                Response response = client.list(20);
                ListJobResponse listJobResponse = (ListJobResponse) response;
                if (listJobResponse.finished().size() == JOB_AMOUNT) {
                    break;
                }
                System.err.println(listJobResponse.running().size() + " jobs still running / "
                        + listJobResponse.queue().size() + " in the queue");
            }
            {
                Response response = client.list(JOB_AMOUNT);
                ListJobResponse listJobResponse = (ListJobResponse) response;
                assertThat(listJobResponse.queue().size(), is(0));
                assertThat(listJobResponse.running().size(), is(0));
                assertThat(listJobResponse.finished().size(), is(JOB_AMOUNT));
            }

            System.err.println("ε=ε=ε=ε=ε=ε= ◟( ⚫͈ω⚫̤)◞ ");

            for (Job job : jobs) {
                Response response = client.getJob(job.id());
                assertThat(response.status(), is("ok"));
                GetJobResponse getJobResponse = (GetJobResponse) response;
                assertTrue(getJobResponse.job().isPresent());
                Job result = getJobResponse.job().get();
                assertThat(result.result(), is(0));
                assertThat(result.state(), is(Job.JobState.FINISHED));
            }
            System.err.println("[SUCCESS] ＼(≧▽≦)／");
        }
    }

    @Test
    public void userTest() throws Exception {
        System.err.println("Connecting to " + RETZ_HOST);
        // create-user, list-user, disable-user, enable-user, get-user
        {
            String[] command = {"java", "-jar", "/build/libs/retz-admin-all.jar", "list-user"};
            verifyCommand(command);
        }
        {
            String[] command = {"java", "-jar", "/build/libs/retz-admin-all.jar", "create-user"};
            verifyCommand(command);
        }
        {
            String[] command = {"java", "-jar", "/build/libs/retz-admin-all.jar", "get-user", "-id", "deadbeef"};
            verifyCommand(command);
        }
        {
            String[] command = {"java", "-jar", "/build/libs/retz-admin-all.jar", "disable-user", "-id", "deadbeef"};
            verifyCommand(command);
        }
        {
            String[] command = {"java", "-jar", "/build/libs/retz-admin-all.jar", "get-user", "-id", "deadbeef"};
            verifyCommand(command);
        }
        {
            String[] command = {"java", "-jar", "/build/libs/retz-admin-all.jar", "fail!"};
            verifyCommandFails(command, "ERROR");
        }
    }

    private void verifyCommand(String[] command) throws  Exception {
        String result = container.system(command);
        assertFalse(result, result.contains("ERROR"));
        assertFalse(result, result.contains("Error"));
    }

    private void verifyCommandFails(String[] command, String word) throws Exception {
        String result = container.system(command);
        assertTrue(result, result.contains(word));
    }
}

