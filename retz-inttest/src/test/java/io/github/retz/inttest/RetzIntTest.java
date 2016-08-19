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

import io.github.retz.cli.TimestampHelper;
import io.github.retz.protocol.*;
import io.github.retz.web.Client;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.*;

/**
 * Simple integration test cases for retz-server / -executor.
 */
public class RetzIntTest extends IntTestBase {
    private static final int RES_OK = 0;

    @Test
    public void listAppTest() throws Exception {
        Client client = new Client(retzServerUri());
        assert (client.connect());
        ListAppResponse response = (ListAppResponse) client.listApp();
        assertEquals("ok", response.status());
        assertEquals(0, response.applicationList().size());
        client.close();
    }

    public <E> void assertIncludes(List<E> list, E element) {
        for (E e : list) {
            if (e.equals(element)) {
                return;
            }
        }
        fail("List does not include element " + element);
    }

    @Test
    public void runAppTest() throws Exception {
        Client client = new Client(retzServerUri());
        assert (client.connect());
        LoadAppResponse loadRes =
                (LoadAppResponse) client.load("echo-app", Arrays.asList(),
                        Arrays.asList("file:///spawn_retz_server.sh"), null);
        assertEquals("ok", loadRes.status());

        ListAppResponse listRes = (ListAppResponse) client.listApp();
        assertEquals("ok", listRes.status());
        List<String> appNameList = listRes.applicationList().stream().map(app -> app.getAppid()).collect(Collectors.toList());
        assertIncludes(appNameList, "echo-app");
        String echoText = "hoge from echo-app via Retz!";
        Job job = new Job("echo-app", "echo " + echoText,
                new Properties(), new Range(1, 2), new Range(128, 256));
        Job runRes = client.run(job);
        assertEquals(RES_OK, runRes.result());

        String baseUrl = baseUrl(runRes);
        String toDir = "build/log/";
        // These downloaded files are not inspected now, useful for debugging test cases, maybe
        Client.fetchHTTPFile(baseUrl, "stdout", toDir);
        Client.fetchHTTPFile(baseUrl, "stderr", toDir);
        Client.fetchHTTPFile(baseUrl, "stdout-" + runRes.id(), toDir);
        Client.fetchHTTPFile(baseUrl, "stderr-" + runRes.id(), toDir);

        String actualText = catStdout(runRes);
        assertEquals(echoText + "\n", actualText);

        ListJobResponse listJobResponse = (ListJobResponse) client.list(64);
        assertTrue(listJobResponse.finished().size() > 0);
        assertEquals(0, listJobResponse.running().size());
        assertEquals(0, listJobResponse.queue().size());

        UnloadAppResponse unloadRes = (UnloadAppResponse) client.unload("echo-app");
        assertEquals("ok", unloadRes.status());

        client.close();
    }

    @Test
    public void scheduleAppTest() throws Exception {
        try (Client client = new Client(retzServerUri())) {

            assert (client.connect());
            LoadAppResponse loadRes =
                    (LoadAppResponse) client.load("echo2",
                            Arrays.asList(),
                            Arrays.asList(),
                            null);
            assertEquals("ok", loadRes.status());

            ListAppResponse listRes = (ListAppResponse) client.listApp();
            assertEquals("ok", listRes.status());
            List<String> appNameList = listRes.applicationList().stream().map(app -> app.getAppid()).collect(Collectors.toList());
            assertIncludes(appNameList, "echo2");

            List<EchoJob> echoJobs = new LinkedList<>();
            List<EchoJob> finishedJobs = new LinkedList<>();
            List<Integer> argvList = IntStream.rangeClosed(0, 32).boxed().collect(Collectors.toList());
            argvList.addAll(Arrays.asList(42, 63, 64, 127, 128, 151, 192, 255));
            int jobNum = argvList.size();
            for (Integer i : argvList) {
                Job job = new Job("echo2", "echo.sh " + i.toString(),
                        new Properties(), new Range(1, 1), new Range(32, 256));

                ScheduleResponse scheduleResponse = (ScheduleResponse) client.schedule(job);
                assertEquals("ok", scheduleResponse.status());

                Job scheduledJob = scheduleResponse.job();
                echoJobs.add(new EchoJob(i.intValue(), scheduledJob));
            }
            assertEquals(jobNum, echoJobs.size());

            for (int i = 0; i < 16; i++) {
                List<EchoJob> toRemove = new LinkedList<>();
                for (EchoJob echoJob : echoJobs) {
                    Response response = client.getJob(echoJob.job.id());
                    if (response instanceof GetJobResponse) {
                        GetJobResponse getJobResponse = (GetJobResponse) client.getJob(echoJob.job.id());
                        // System.err.println(echoJob.job.id() + " => " + getJobResponse.job().get().result());
                        if (getJobResponse.job().isPresent()) {
                            if (getJobResponse.job().get().result() == echoJob.argv) {
                                toRemove.add(echoJob);
                                assertEquals(Integer.toString(echoJob.argv) + "\n",
                                        catStdout(getJobResponse.job().get()));
                            } else {
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
                if (!toRemove.isEmpty()) {
                    i = 0;
                }
                echoJobs.removeAll(toRemove);
                finishedJobs.addAll(toRemove);
                if (echoJobs.isEmpty()) {
                    break;
                }
                Thread.sleep(1000);

                ListJobResponse listJobResponse = (ListJobResponse) client.list(64);
                System.err.println(TimestampHelper.now()
                        + ": Finished=" + listJobResponse.finished().size()
                        + ", Running=" + listJobResponse.running().size()
                        + ", Scheduled=" + listJobResponse.queue().size());
            }
            assertEquals(jobNum, finishedJobs.size());

            ListJobResponse listJobResponse = (ListJobResponse) client.list(64);
            assertTrue(listJobResponse.finished().size() >= finishedJobs.size());
            assertEquals(0, listJobResponse.running().size());
            assertEquals(0, listJobResponse.queue().size());

            UnloadAppResponse unloadRes = (UnloadAppResponse) client.unload("echo-app");
            assertEquals("ok", unloadRes.status());

        }
    }

    private String catStdout(Job job) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        String baseUrl = baseUrl(job);
        Client.catHTTPFile(baseUrl, "stdout-" + job.id(), out);
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
