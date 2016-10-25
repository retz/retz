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
package io.github.retz.scheduler;

import io.github.retz.cli.FileConfiguration;
import io.github.retz.cli.TimestampHelper;
import io.github.retz.protocol.data.Application;
import io.github.retz.protocol.data.Job;
import io.github.retz.protocol.data.MesosContainer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class JobQueueTest {
    @Before
    public void before() throws Exception {
        InputStream in = Launcher.class.getResourceAsStream("/retz-tls.properties");

        FileConfiguration config = new FileConfiguration(in);
        Database.getInstance().getInstance().init(config);
    }
    @After
    public void after() throws Exception {
        Database.getInstance().deleteAllJob(Integer.MAX_VALUE);
        Database.getInstance().deleteAllProperties();
        Database.getInstance().stop();
    }
    @Test
    public void q() throws InterruptedException, IOException {
        Application app = new Application("a", Arrays.asList(), Arrays.asList(), Arrays.asList(),
                Optional.empty(), Optional.empty(), "deadbeef", new MesosContainer(), true);
        Applications.load(app);
        Job job = new Job("a", "b", null, 1000, 100000000);
        job.schedule(0, TimestampHelper.now());
        JobQueue.push(job);
        {
            List<Job> job2 = JobQueue.findFit(1001, 100000001);
            for (Job j : job2) {
                System.err.println(job.appid() + job.name());
            }
            System.err.println(job2.size());
            assertFalse(job2.isEmpty());
            assertThat(job2.get(0).appid(), is(job.appid()));
            JobQueue.starting(job2.get(0), Optional.empty(), "foobar-taskid");

            Optional<Job> j = Database.getInstance().getJobFromTaskId("foobar-taskid");
            assertTrue(j.isPresent());
        }

        {
            JobQueue.started("foobar-taskid", Optional.empty());
            List<Job> fit = JobQueue.findFit(1000, 100000000);
            for (Job j : fit) {
                System.err.println(j.name() + " " +j.cmd());
            }
            assertTrue(fit.isEmpty());
            assertThat(JobQueue.countRunning(), is(1));
        }
        assertEquals(1, JobQueue.size());
        assertEquals(1, JobQueue.countRunning());
    }
}
