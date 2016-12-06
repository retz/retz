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
package io.github.retz.protocol;

import io.github.retz.cli.TimestampHelper;
import io.github.retz.protocol.data.Job;
import org.junit.Before;
import org.junit.Test;

import java.util.Optional;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

public class JobTest {

    private Job job;

    @Before
    public void before() {
        String scheduled = TimestampHelper.now();
        Properties props = new Properties();
        props.setProperty("LANG", "C");
        props.setProperty("ASAKUSA_HOME", "/var/lib/asakusa/");
        props.setProperty("KEY_SECRET", "deadbeef0096");
        job = new Job(
                "bin/yaess-batch.sh m3bp.example.sales -date ....",
                scheduled,
                null,
                null,
                props,
                -42,
                10000042,
                "https://example.com:5050/path/to/sandbox",
                "hey, aren't you a Mesos task?? (<=reason)",
                0,
                0,
                "sample-app",
                null,
                32,
                65536,
                8,
                0,
                0,
                "my-sample-taskid",
                Job.JobState.CREATED);
        job.setName("job=name");
        job.setPriority(-10);
    }


    @Test
    public void ppQueued() {
        job.schedule(100000042, TimestampHelper.now());
        System.err.println(job.toString());
        System.err.println(job.pp());
        assertEquals(Job.JobState.QUEUED, job.state());
    }

    @Test
    public void ppStarting() {
        job.starting("retz-new-task-id-2354", Optional.empty(), TimestampHelper.now());
        System.err.println(job.toString());
        System.err.println(job.pp());
        assertEquals(Job.JobState.STARTING, job.state());
    }

    @Test public void ppStarted() {
        job.started("retz-new-task-id-2354", Optional.empty(), TimestampHelper.now());
        System.err.println(job.toString());
        System.err.println(job.pp());
        assertEquals(Job.JobState.STARTED, job.state());
    }

    @Test public void ppFinished() {
        job.finished(TimestampHelper.now(), Optional.empty(), 0);
        System.err.println(job.toString());
        System.err.println(job.pp());
        assertEquals(Job.JobState.FINISHED, job.state());
    }

    @Test public void ppKilled() {
        job.killed(TimestampHelper.now(), Optional.empty(), "deadly important stupid some reason");
        System.err.println(job.toString());
        System.err.println(job.pp());
        assertEquals(Job.JobState.KILLED, job.state());
    }

    @Test
    public void ppRetry() {
        job.doRetry();
        System.err.println(job.toString());
        System.err.println(job.pp());
        assertEquals(Job.JobState.QUEUED, job.state());
    }
}
