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
package io.github.retz.scheduler;

import io.github.retz.cli.TimestampHelper;
import io.github.retz.db.Database;
import io.github.retz.protocol.data.Application;
import io.github.retz.protocol.data.Job;
import io.github.retz.protocol.data.MesosContainer;
import io.github.retz.protocol.data.ResourceQuantity;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

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

        ServerConfiguration config = new ServerConfiguration(in);
        Database.getInstance().getInstance().init(config);
    }
    @After
    public void after() throws Exception {
        Database.getInstance().deleteAllJob(Integer.MAX_VALUE);
        Database.getInstance().deleteAllProperties();
        Database.getInstance().clear(); // Deleting users and applications?
        Database.getInstance().stop();
    }
    @Test
    public void q() throws Exception {
        Application app = new Application("appq", Arrays.asList(), Arrays.asList(),
                Optional.empty(), "deadbeef",0, new MesosContainer(), true);
        assertTrue(Applications.load(app));

        Job job = new Job("appq", "b", null, 1000, 100000000, 0);
        job.schedule(0, TimestampHelper.now());
        JobQueue.push(job);
        {
            List<Job> job2 = JobQueue.findFit(Arrays.asList("id"), new ResourceQuantity(1001, 100000001, 0, 0, 0, 0));
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
            List<Job> fit = JobQueue.findFit(Arrays.asList("id"), new ResourceQuantity(1000, 100000000, 0, 0, 0, 0));
            for (Job j : fit) {
                System.err.println(j.name() + " " +j.cmd());
            }
            assertTrue(fit.isEmpty());
            assertThat(JobQueue.countRunning(), is(1));
        }
        assertEquals(1, JobQueue.size());
        assertEquals(1, JobQueue.countRunning());
        Database.getInstance().safeDeleteApplication(app.getAppid());
    }
}
