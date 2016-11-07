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
import io.github.retz.cli.TimestampHelper;
import io.github.retz.protocol.*;
import io.github.retz.protocol.data.Application;
import io.github.retz.protocol.data.Job;
import io.github.retz.protocol.data.MesosContainer;
import io.github.retz.web.Client;
import io.github.retz.web.ClientHelper;
import org.junit.*;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.github.retz.inttest.IntTestBase.*;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

/**
 * Simple integration test cases for retz-server / -executor.
 */
public class RetzIntTest {
    private static final int RES_OK = 0;
    private static ClosableContainer container;
    private static final String configfile = "retz-client.properties";
    protected ClientCLIConfig config;

    @BeforeClass
    public static void setupContainer() throws Exception {
        System.out.println(Client.VERSION_STRING);
        container = createContainer(CONTAINER_NAME);
        container.setConfigfile("retz.properties");
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
        config = new ClientCLIConfig("src/test/resources/" + configfile);
        assertEquals(RETZ_HOST, config.getUri().getHost());
        assertEquals(RETZ_PORT, config.getUri().getPort());
    }

    @Test
    public void listAppTest() throws Exception {
        URI uri = new URI("http://" + RETZ_HOST + ":" + RETZ_PORT);
        Client client = Client.newBuilder(uri)
                .enableAuthentication(config.authenticationEnabled())
                .setAuthenticator(config.getAuthenticator())
                .build();
        Response res = client.listApp();
        System.out.println(res.status());
        ListAppResponse response = (ListAppResponse) client.listApp();
        assertThat(response.status(), is("ok"));
        assertThat(response.applicationList().size(), greaterThanOrEqualTo(0));
        client.close();
    }

    public <E> void assertIncludes(List<E> list, E element) {
        assertThat(list, hasItem(element));
    }

    @Test
    public void runAppTest() throws Exception {
        URI uri = new URI("http://" + RETZ_HOST + ":" + RETZ_PORT);
        Client client = Client.newBuilder(uri)
                .enableAuthentication(config.authenticationEnabled())
                .setAuthenticator(config.getAuthenticator())
                .build();
        Application echoApp = new Application("echo-app", Arrays.asList(), Arrays.asList(), Arrays.asList("file:///spawn_retz_server.sh"),
                Optional.empty(), Optional.empty(), "deadbeef", new MesosContainer(), true);
        LoadAppResponse loadRes =
                (LoadAppResponse) client.load(echoApp);
        assertThat(loadRes.status(), is("ok"));

        ListAppResponse listRes = (ListAppResponse) client.listApp();
        assertThat(listRes.status(), is("ok"));
        List<String> appNameList = listRes.applicationList().stream().map(app -> app.getAppid()).collect(Collectors.toList());
        assertIncludes(appNameList, "echo-app");
        String echoText = "hoge from echo-app via Retz!";
        Job job = new Job("echo-app", "echo " + echoText, new Properties(), 2, 256);
        Job runRes = client.run(job);
        assertThat(runRes.result(), is(RES_OK));
        assertThat(runRes.state(), is(Job.JobState.FINISHED));

        String toDir = "build/log/";
        // These downloaded files are not inspected now, useful for debugging test cases, maybe

        ClientHelper.getWholeFile(client, runRes.id(), "stdout", toDir);
        ClientHelper.getWholeFile(client, runRes.id(), "stderr", toDir);

        String actualText = catStdout(client, runRes);
        List<String> lines = Arrays.asList(actualText.split("\n"));
        assertThat(lines, hasItem(echoText));
        assertThat(lines, hasItem("Received SUBSCRIBED event"));
        assertThat(lines, hasItem("Received LAUNCH event"));

        ListJobResponse listJobResponse = (ListJobResponse) client.list(64);
        assertThat(listJobResponse.finished().size(), greaterThan(0));
        assertThat(listJobResponse.running().size(), is(0));
        assertThat(listJobResponse.queue().size(), is(0));

        UnloadAppResponse unloadRes = (UnloadAppResponse) client.unload("echo-app");
        assertThat(unloadRes.status(), is("ok"));

        client.close();
    }

    @Test
    public void killTest() throws Exception {
        URI uri = new URI("http://" + RETZ_HOST + ":" + RETZ_PORT);
        Client client = Client.newBuilder(uri)
                .enableAuthentication(config.authenticationEnabled())
                .setAuthenticator(config.getAuthenticator())
                .build();

        Application echoApp = new Application("echo-app", Arrays.asList(), Arrays.asList(), Arrays.asList(),
                Optional.empty(), Optional.empty(), "deadbeef", new MesosContainer(), true);
        Response response = client.load(echoApp);
        System.err.println(response.status());
        LoadAppResponse loadRes = (LoadAppResponse) response;
        assertThat(loadRes.status(), is("ok"));

        ListAppResponse listRes = (ListAppResponse) client.listApp();
        assertThat(listRes.status(), is("ok"));
        List<String> appNameList = listRes.applicationList().stream().map(app -> app.getAppid()).collect(Collectors.toList());
        assertIncludes(appNameList, "echo-app");
        Job job = new Job("echo-app", "sleep 60", new Properties(), 1, 32);
        response = client.schedule(job);
        assertThat(response, instanceOf(ScheduleResponse.class));
        ScheduleResponse scheduleResponse = (ScheduleResponse) response;
        int id = scheduleResponse.job().id();
        System.err.println(id);

        {
            response = client.getJob(id);
            assertThat(response, instanceOf(GetJobResponse.class));
            GetJobResponse getJobResponse = (GetJobResponse) response;
            //depending on the timing, like if the test is relatively slow, this could be JobState.STARTED
            assertTrue(getJobResponse.job().isPresent());
            assertThat(getJobResponse.job().get().state(), isOneOf(Job.JobState.QUEUED, Job.JobState.STARTING));
        }
        {
            response = client.kill(id);
            System.err.println(response.status());
            assertThat(response, instanceOf(KillResponse.class));
        }
        {
            response = client.getJob(id);
            assertThat(response, instanceOf(GetJobResponse.class));
            GetJobResponse getJobResponse = (GetJobResponse) response;
            Thread.sleep(10000);
            assertThat(getJobResponse.job().get().state(), is(Job.JobState.KILLED));
        }

        UnloadAppResponse unloadRes = (UnloadAppResponse) client.unload("echo-app");
        assertThat(unloadRes.status(), is("ok"));

        client.close();
    }

    @Test
    public void scheduleAppTest() throws Exception {
        URI uri = new URI("http://" + RETZ_HOST + ":" + RETZ_PORT);

        try (Client client = Client.newBuilder(uri)
                .enableAuthentication(config.authenticationEnabled())
                .setAuthenticator(config.getAuthenticator())
                .build()) {
            loadSimpleApp(client, "echo2");

            List<EchoJob> finishedJobs = new LinkedList<>();
            List<Integer> argvList = IntStream.rangeClosed(0, 32).boxed().collect(Collectors.toList());
            argvList.addAll(Arrays.asList(42, 63, 64, 127, 128, 151, 192, 255));
            int jobNum = argvList.size();
            List<EchoJob> echoJobs = scheduleEchoJobs(client, "echo2", "echo.sh ", argvList);

            assertThat(echoJobs.size(), is(jobNum));

            for (int i = 0; i < 16; i++) {
                List<EchoJob> toRemove = toRemove(client, echoJobs, true);

                if (!toRemove.isEmpty()) {
                    i = 0;
                }
                echoJobs.removeAll(toRemove);
                finishedJobs.addAll(toRemove);
                if (echoJobs.isEmpty()) {
                    break;
                }
                Thread.sleep(1000);

                Response res = client.list(64);
                if (!(res instanceof ListJobResponse)) {
                    ErrorResponse errorResponse = (ErrorResponse) res;
                    System.err.println("Error: " + errorResponse.status());
                    continue;
                }
                ListJobResponse listJobResponse = (ListJobResponse) client.list(64);
                System.err.println(TimestampHelper.now()
                        + ": Finished=" + listJobResponse.finished().size()
                        + ", Running=" + listJobResponse.running().size()
                        + ", Scheduled=" + listJobResponse.queue().size());
                for (Job finished : listJobResponse.finished()) {
                    assertThat(finished.retry(), is(0));
                }
            }
            assertThat(finishedJobs.size(), is(jobNum));

            ListJobResponse listJobResponse = (ListJobResponse) client.list(64);
            assertThat(listJobResponse.finished().size(), greaterThanOrEqualTo(finishedJobs.size()));
            assertThat(listJobResponse.running().size(), is(0));
            assertThat(listJobResponse.queue().size(), is(0));

            UnloadAppResponse unloadRes = (UnloadAppResponse) client.unload("echo2");
            assertThat(unloadRes.status(), is("ok"));
        }
    }

    private void loadSimpleApp(Client client, String appName) throws IOException {
        Application app = new Application(appName, Arrays.asList(),
                Arrays.asList(), Arrays.asList(), Optional.empty(), Optional.empty(), "deadbeef", new MesosContainer(), true);
        Response res = client.load(app);

        assertTrue(res.status(), res instanceof LoadAppResponse);
        LoadAppResponse loadRes = (LoadAppResponse) res;
        assertThat(loadRes.status(), is("ok"));

        ListAppResponse listRes = (ListAppResponse) client.listApp();
        assertThat(listRes.status(), is("ok"));
        List<String> appNameList = listRes.applicationList().stream().map(a -> a.getAppid()).collect(Collectors.toList());
        assertIncludes(appNameList, appName);
    }

    private List<EchoJob> scheduleEchoJobs(Client client, String appName, String cmdPrefix, List<Integer> argvList) throws IOException {
        List<EchoJob> echoJobs = new LinkedList<>();
        for (Integer i : argvList) {
            Job job = new Job(appName, cmdPrefix + i.toString(), new Properties(), 1, 32);

            ScheduleResponse scheduleResponse = (ScheduleResponse) client.schedule(job);
            assertThat(scheduleResponse.status(), is("ok"));

            Job scheduledJob = scheduleResponse.job();
            echoJobs.add(new EchoJob(i.intValue(), scheduledJob));
        }
        return echoJobs;
    }

    private List<EchoJob> toRemove(Client client, List<EchoJob> echoJobs, boolean checkRetval) throws IOException, Exception {
        List<EchoJob> toRemove = new LinkedList<>();
        for (EchoJob echoJob : echoJobs) {
            Response response = client.getJob(echoJob.job.id());
            if (response instanceof GetJobResponse) {
                GetJobResponse getJobResponse = (GetJobResponse) client.getJob(echoJob.job.id());
                // System.err.println(echoJob.job.id() + " => " + getJobResponse.job().get().result());
                if (getJobResponse.job().isPresent()) {
                    if (getJobResponse.job().get().state() == Job.JobState.FINISHED
                            || getJobResponse.job().get().state() == Job.JobState.KILLED) {
                        toRemove.add(echoJob);

                        String actualText = catStdout(client, getJobResponse.job().get());
                        List<String> lines = Arrays.asList(actualText.split("\n"));
                        assertThat(lines, hasItem(Integer.toString(echoJob.argv)));
                        assertThat(lines, hasItem("Received SUBSCRIBED event"));
                        assertThat(lines, hasItem("Received LAUNCH event"));

                    } else if (checkRetval) {
                        assertNull("Unexpected return value for Job " + getJobResponse.job().get().result()
                                        + ", Message: " + getJobResponse.job().get().reason(),
                                getJobResponse.job().get().finished());
                    }
                }
            } else {
                ErrorResponse errorResponse = (ErrorResponse) response;
                System.err.println(echoJob.job.id() + ": " + errorResponse.status());
            }
        }
        return toRemove;
    }

    @Test
    public void scheduleAppTest2() throws Exception {
        URI uri = new URI("http://" + RETZ_HOST + ":" + RETZ_PORT);
        try (Client client = Client.newBuilder(uri)
                .enableAuthentication(config.authenticationEnabled())
                .setAuthenticator(config.getAuthenticator())
                .build()) {
            loadSimpleApp(client, "echo3");

            List<EchoJob> finishedJobs = new LinkedList<>();
            List<Integer> argvList = IntStream.rangeClosed(0, 32).boxed().collect(Collectors.toList());
            argvList.addAll(Arrays.asList(42, 63, 64, 127, 128, 151, 192, 255));
            int jobNum = argvList.size();
            List<EchoJob> echoJobs = scheduleEchoJobs(client, "echo3", "echo ", argvList);
            assertThat(echoJobs.size(), is(jobNum));

            for (int i = 0; i < 16; i++) {
                List<EchoJob> toRemove = toRemove(client, echoJobs, false);
                if (!toRemove.isEmpty()) {
                    i = 0;
                }
                echoJobs.removeAll(toRemove);
                finishedJobs.addAll(toRemove);
                if (echoJobs.isEmpty()) {
                    break;
                }
                Thread.sleep(1000);

                Response res = client.list(64);
                if (!(res instanceof ListJobResponse)) {
                    ErrorResponse errorResponse = (ErrorResponse) res;
                    System.err.println("Error: " + errorResponse.status());
                    continue;
                }
                ListJobResponse listJobResponse = (ListJobResponse) client.list(64);
                System.err.println(TimestampHelper.now()
                        + ": Finished=" + listJobResponse.finished().size()
                        + ", Running=" + listJobResponse.running().size()
                        + ", Scheduled=" + listJobResponse.queue().size());
                for (Job finished : listJobResponse.finished()) {
                    assertThat(finished.retry(), is(0));
                    assertThat(finished.state(), is(Job.JobState.FINISHED));
                    assertThat(finished.result(), is(RES_OK));
                }
            }
            assertThat(finishedJobs.size(), is(jobNum));

            ListJobResponse listJobResponse = (ListJobResponse) client.list(64);
            assertThat(listJobResponse.finished().size(), greaterThanOrEqualTo(finishedJobs.size()));
            assertThat(listJobResponse.running().size(), is(0));
            assertThat(listJobResponse.queue().size(), is(0));

            UnloadAppResponse unloadRes = (UnloadAppResponse) client.unload("echo3");
            assertThat(unloadRes.status(), is("ok"));
        }
    }

    private String catStdout(Client c, Job job) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ClientHelper.getWholeFile(c, job.id(), "stdout", true, out);
        return out.toString(String.valueOf(StandardCharsets.UTF_8));
    }

    private String baseUrl(Job job) throws Exception {
        URL resUrl = new URL(job.url());
        // Rewrite HOST (IP) part to access without bridge interface in Docker for Mac
        return (new URL(resUrl.getProtocol(), "127.0.0.1", resUrl.getPort(), resUrl.getFile())).toString();
    }

    static class EchoJob {
        int argv;
        Job job;

        EchoJob(int argv, Job job) {
            this.argv = argv;
            this.job = job;
        }
    }

}
