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
package io.github.retz.protocol;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import io.github.retz.cli.TimestampHelper;
import io.github.retz.protocol.data.Job;
import io.github.retz.protocol.data.ResourceQuantity;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
                "job-name-foobar",
                new HashSet<String>(),
                new ResourceQuantity(32, 65536, 8, 0, 0, 0),
                Optional.empty(),
                "my-sample-taskid",
                null,
                Job.JobState.CREATED);
        job.setName("job=name");
        job.setPriority(-10);
        job.addTags(Arrays.asList("tag1", "tag2"));
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

    @Test
    public void ppStarted() {
        job.started("retz-new-task-id-2354", "me is new slave", Optional.empty(), TimestampHelper.now());
        System.err.println(job.toString());
        System.err.println(job.pp());
        assertEquals(Job.JobState.STARTED, job.state());
    }

    @Test
    public void ppFinished() {
        job.finished(TimestampHelper.now(), Optional.empty(), 0);
        System.err.println(job.toString());
        System.err.println(job.pp());
        assertEquals(Job.JobState.FINISHED, job.state());
    }

    @Test
    public void ppKilled() {
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

    @Test
    public void maskPropertiesTest() {
        Properties props = job.props();
        List<String> secretProperties = Arrays.asList("some_secret_string", "SOME_SECRET_STRING",
                "some_password_string", "SOME_PASSWORD_STRING",
                "some_token_string", "SOME_TOKEN_STRING");
        secretProperties.forEach(k -> {
            props.setProperty(k, "%STRING_TO_REPLACE%");
        });
        List<String> otherProperties = Arrays.asList("some_other_string", "SOME_OTHER_STRING");
        otherProperties.forEach(k -> {
            props.setProperty(k, "%STRING_NOT_TO_REPLACE%");
        });

        Arrays.asList(job.toString(), job.pp()).forEach(s -> {
            System.err.println(s);
            secretProperties.forEach(k -> {
                Pattern p = Pattern.compile(String.format("^.*%s=([^,}]*).*$", k));
                Matcher m = p.matcher(s);
                assert(m.matches());
                assertEquals(m.group(1), "<masked>");
            });
            otherProperties.forEach(k -> {
                Pattern p = Pattern.compile(String.format("^.*%s=([^,}]*).*$", k));
                Matcher m = p.matcher(s);
                assert(m.matches());
                assertEquals(m.group(1), "%STRING_NOT_TO_REPLACE%");
            });
        });
    }

    @Test
    public void formatCompat() throws IOException {

        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new Jdk8Module());
        System.err.println(mapper.writeValueAsString(job));

        {
            // This is a JSON-encoded 'job' WITHOUT null entries
            String json = "{\"cmd\":\"bin/yaess-batch.sh m3bp.example.sales -date ....\",\"scheduled\":\"2017-08-31T17:36:46.431+09:00\",\"started\":null,\"finished\":null,\"result\":-42,\"id\":10000042,\"url\":\"https://example.com:5050/path/to/sandbox\",\"reason\":\"hey, aren't you a Mesos task?? (<=reason)\",\"retry\":0,\"priority\":-10,\"appid\":\"sample-app\",\"name\":\"job=name\",\"tags\":[\"tag1\",\"tag2\"],\"resources\":{\"cpu\":32,\"memMB\":65536,\"gpu\":8,\"ports\":0,\"diskMB\":0,\"nodes\":0},\"taskId\":\"my-sample-taskid\",\"state\":\"CREATED\",\"props\":{}}";
            // And test whether it has backward compatibility with old job formats, before "slaveId" addition
            Job job2 = mapper.readValue(json, Job.class);
            System.err.println(job2.pp());
        }
    }
}
