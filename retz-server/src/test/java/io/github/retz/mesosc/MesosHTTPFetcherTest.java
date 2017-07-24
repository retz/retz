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
package io.github.retz.mesosc;

import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertTrue;

/**
 * @doc JSON parsing test
 */
public class MesosHTTPFetcherTest {
    @Test
    public void parseTypicalJSON() throws IOException {
        InputStream in;
        Optional<String> s;

        in = MesosHTTPFetcherTest.class.getResourceAsStream("/master-slaves.json");
        s = MesosHTTPFetcher.extractSlaveAddr(in, "6c751ae7-6856-4127-aea1-42f3a9210846-S0");
        assertTrue(s.isPresent());
        assertThat(s.get(), is("127.0.0.1:5051"));

        in = MesosHTTPFetcherTest.class.getResourceAsStream("/slave-flags.json");
        s = MesosHTTPFetcher.extractSlaveBasePath(in);
        assertTrue(s.isPresent());
        assertThat(s.get(), is("/tmp/mesos"));
    }

    @Test
    public void parseSlaveState() throws IOException {
        verify("3a3e9491-84a5-4c9d-8fed-5ca10c23d922-0000", "sum", "d9b0e1d9-49dc-4dd1-8773-d53366fc1517",
                "/tmp/mesos/slaves/6c751ae7-6856-4127-aea1-42f3a9210846-S0/frameworks/3a3e9491-84a5-4c9d-8fed-5ca10c23d922-0000/executors/sum/runs/d9b0e1d9-49dc-4dd1-8773-d53366fc1517");

        verify("3a3e9491-84a5-4c9d-8fed-5ca10c23d922-0000", "sum", "48308555-7d5a-4c37-8c50-db9bcf58c637",
                "/tmp/mesos/slaves/6c751ae7-6856-4127-aea1-42f3a9210846-S0/frameworks/3a3e9491-84a5-4c9d-8fed-5ca10c23d922-0000/executors/sum/runs/48308555-7d5a-4c37-8c50-db9bcf58c637");

        verify("3a3e9491-84a5-4c9d-8fed-5ca10c23d922-0000", "sum", "927b4c8a-bcfb-40fb-bf24-fcd4a430e2aa",
                "/tmp/mesos/slaves/6c751ae7-6856-4127-aea1-42f3a9210846-S0/frameworks/3a3e9491-84a5-4c9d-8fed-5ca10c23d922-0000/executors/sum/runs/927b4c8a-bcfb-40fb-bf24-fcd4a430e2aa");
    }

    private void verify(String frameworkId, String executorId, String containerId, String directory) throws IOException {
        try (InputStream in = MesosHTTPFetcherTest.class.getResourceAsStream("/slave-state.json")) {
            Optional<String> s = MesosHTTPFetcher.extractDirectory(in, frameworkId, executorId, containerId);
            assertTrue(s.isPresent());
            assertThat(s.get(), is(directory));
        }
    }

    @Test
    public void parseSlaveState2() throws IOException {
        verify2("3a3e9491-84a5-4c9d-8fed-5ca10c23d922-0000", "sum", "retz-task-id-0",
                "/tmp/mesos/slaves/6c751ae7-6856-4127-aea1-42f3a9210846-S0/frameworks/3a3e9491-84a5-4c9d-8fed-5ca10c23d922-0000/executors/sum/runs/d9b0e1d9-49dc-4dd1-8773-d53366fc1517");

        verify2("3a3e9491-84a5-4c9d-8fed-5ca10c23d922-0000", "sum", "retz-task-id-1",
                "/tmp/mesos/slaves/6c751ae7-6856-4127-aea1-42f3a9210846-S0/frameworks/3a3e9491-84a5-4c9d-8fed-5ca10c23d922-0000/executors/sum/runs/48308555-7d5a-4c37-8c50-db9bcf58c637");

        verify2("3a3e9491-84a5-4c9d-8fed-5ca10c23d922-0000", "sum", "retz-task-id-2",
                "/tmp/mesos/slaves/6c751ae7-6856-4127-aea1-42f3a9210846-S0/frameworks/3a3e9491-84a5-4c9d-8fed-5ca10c23d922-0000/executors/sum/runs/927b4c8a-bcfb-40fb-bf24-fcd4a430e2aa");
    }

    private void verify2(String frameworkId, String executorId, String taskId, String directory) throws IOException {
        System.err.println("extracting frameworkId=" + frameworkId + " executorId=" + executorId + " taskId=" + taskId);
        try (InputStream in = MesosHTTPFetcherTest.class.getResourceAsStream("/slave-state.json")) {
            Optional<String> s = MesosHTTPFetcher.extractDirectory2(in, frameworkId, executorId, taskId);
            System.err.println(s);
            assertTrue(s.isPresent());
            assertThat(s.get(), is(directory));
        }
    }

    @Test
    public void parseTask() throws IOException {
        InputStream in = MesosHTTPFetcherTest.class.getResourceAsStream("/master-tasks.json");
        List<Map<String, Object>> result = MesosHTTPFetcher.parseTasks(in, "bcc4637f-34d7-4cab-8429-7146fabd7198-0001");
        System.err.println(result);
        assertThat(result.size(), is(1));
        assertThat(result.get(0).get("state"), is("TASK_FINISHED"));
    }
}
