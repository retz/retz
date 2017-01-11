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

import io.github.retz.protocol.*;
import io.github.retz.protocol.data.Application;
import io.github.retz.protocol.data.Job;
import io.github.retz.protocol.data.MesosContainer;
import io.github.retz.protocol.data.User;
import io.github.retz.web.Client;
import io.github.retz.web.ClientHelper;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.net.URI;
import java.util.*;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

@Ignore
public class PersistenceTest extends RetzIntTest {
    private static final int JOB_AMOUNT = 64;
    String serverConfigFile;

    @Before
    public void before() {
        Objects.requireNonNull(serverConfigFile);
    }

    @Test
    public void persistence() throws Exception {
        User user = config.getUser();
        List<String> e = Arrays.asList();
        Application application = new Application("t", e, e, Optional.empty(),
                user.keyId(), 0, new MesosContainer(), true);
        URI uri = new URI("http://" + RETZ_HOST + ":" + RETZ_PORT);

        try (Client client = Client.newBuilder(uri).setAuthenticator(config.getAuthenticator()).build()) {

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
                List<Job> finished, queue, running;
                finished = ClientHelper.finished(client);
                queue = ClientHelper.queue(client);
                running = ClientHelper.running(client);
                System.err.println("Finished: " + finished.size());
                System.err.println("Queued: " + queue.size());
                System.err.println("Running: " + running.size());
                assertThat(finished.size() + queue.size() + running.size(), is(JOB_AMOUNT));
            }

            boolean killed = container.killRetzServerProcess();
            assertTrue(killed);

            container.startRetzServer(serverConfigFile);
            container.waitForRetzServer();

            checkApp(uri, application, jobs);
        }
    }

    void checkApp(URI uri, Application application, List<Job> jobs) throws Exception {
        try (Client client = Client.newBuilder(uri).setAuthenticator(config.getAuthenticator()).build()) {
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
                List<Job> finished, queue, running;
                finished = ClientHelper.finished(client);
                queue = ClientHelper.queue(client);
                running = ClientHelper.running(client);
                System.err.println("Finished: " + finished.size());
                System.err.println("Queued: " + queue.size());
                System.err.println("Running: " + running.size());
            }

            for (int i = 0; i < JOB_AMOUNT / 2; i++) {
                Thread.sleep(4 * 1024);
                List<Job> finished = ClientHelper.finished(client);
                if (finished.size() == JOB_AMOUNT) {
                    break;
                }
                List<Job> running = ClientHelper.running(client);
                List<Job> queue = ClientHelper.queue(client);
                System.err.println(running.size() + " jobs still running / "
                        + queue.size() + " in the queue");
            }
            {
                List<Job> finished, queue, running;
                finished = ClientHelper.finished(client);
                queue = ClientHelper.queue(client);
                running = ClientHelper.running(client);
                assertThat(queue.size(), is(0));
                assertThat(running.size(), is(0));
                assertThat(finished.size(), is(JOB_AMOUNT));
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


}

