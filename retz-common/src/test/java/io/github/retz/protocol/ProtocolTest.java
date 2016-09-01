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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Optional;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

// TODO: introduce ScalaCheck for further tests here
public class ProtocolTest {

    ObjectMapper mapper;

    @Before
    public void setUp() {
        mapper = new ObjectMapper();
        mapper.registerModule(new Jdk8Module());
    }

    @After
    public void tearDown() {
    }

    @Test
    public void decodeObjects() throws IOException {
        {
            String jobString = "{ \"appid\":\"myid\", \"cmd\":\"yaess/bin/yaess-batch.sh ....\", \"scheduled\":\"2016-05-15:20:20:20Z\", \"started\":\"2016-05-15:20:20:20Z\", \"finished\":\"2016-05-15:20:20:20Z\", \"result\":0, \"id\":1, \"url\":\"https://....\"}";
            Job job = mapper.readValue(jobString, Job.class);
            assertThat(job.cmd(), is("yaess/bin/yaess-batch.sh ...."));
            assertThat(job.result(), is(0));
            assertThat(job.id(), is(1));
        }
        {
            String jobString = "{ \"appid\" : \"myid\", \"cmd\":\"yaess/bin/yaess-batch.sh ....\", \"scheduled\":\"2016-05-15:20:20:20Z\", \"id\":2}";
            Job job = mapper.readValue(jobString, Job.class);
            assertThat(job.cmd(), is("yaess/bin/yaess-batch.sh ...."));
            assertNull(job.started());
            assertThat(job.result(), is(0));
        }
        {
            String appString = "{\"appid\":\"my-one-of-42-apps\", \"files\":[\"http://example.com/app.tar.gz\"]}";
            Application app = mapper.readValue(appString, Application.class);
            assertThat(app.getAppid(), is("my-one-of-42-apps"));
            assertNotNull(app.getFiles());
            assertThat(app.getFiles().size(), is(1));
            assertFalse(app.getDiskMB().isPresent());
        }
        {
            String appString = "{\"appid\":\"foobar\",\"files\":[\"http://example.com:234/foobar/test.tar.gz\"], \"diskMB\":null}";
            Application app = mapper.readValue(appString, Application.class);
            assertFalse(app.getDiskMB().isPresent());
        }
        {
            String metajobString = "{\"job\":{\"cmd\":\"touch /tmp/junit3807420247585460493.tmp\",\"scheduled\":null,\"started\":null,\"finished\":null,\"result\":-1,\"id\":0,\"url\":null,\"appid\":\"appname\",\"name\":null,\"cpu\":{\"min\":1,\"max\":2147483647},\"memMB\":{\"min\":128,\"max\":2147483647},\"props\":null},\"app\":{\"appid\":\"appname\",\"persistentFiles\":[],\"files\":[],\"diskMB\":null}}";
            MetaJob metaJob = mapper.readValue(metajobString, MetaJob.class);
            assertNotNull(metaJob.getApp());
        }
    }

    public void decodeRequests() throws IOException {

        {
            String json = "{\"command\":\"list\"}";
            Request req = mapper.readValue(json, Request.class);
            assertThat(req, instanceOf(ListJobRequest.class));
        }
        {
            String json = "{\"command\":\"schedule\",\"job\":{\"cmd\":\"ls -l\"}}";
            Request req = mapper.readValue(json, Request.class);
            assertThat(req, instanceOf(ScheduleRequest.class));
            ScheduleRequest sreq = (ScheduleRequest) req;
            assertNotNull(sreq.job());
            assertThat(sreq.job().cmd(), is("ls -l"));
        }
        {
            String json = "{\"command\":\"schedule\",\"job\":{\"cmd\":\"Mmmmmmmmmy commmmmand1!!!!!\",\"scheduled\":null,\"started\":null,\"finished\":null,\"result\":0,\"id\":0,\"url\":null}}";
            Request req = mapper.readValue(json, Request.class);
            assertThat(req, instanceOf(ScheduleRequest.class));
            ScheduleRequest sreq = (ScheduleRequest) req;
            assertNotNull(sreq.job());
            assertThat(sreq.job().cmd(), is("Mmmmmmmmmy commmmmand1!!!!!"));
        }
        {
            String json = "{\"command\":\"get-job\", \"id\":23}";
            Request req = mapper.readValue(json, Request.class);
            assertThat(req, instanceOf(GetJobRequest.class));
            GetJobRequest getJobRequest = (GetJobRequest)req;
            assertThat(23, is(getJobRequest.id()));
        }

        {
            String json = "{\"command\":\"kill\",\"id\":2}}";
            Request req = mapper.readValue(json, Request.class);
            assertThat(req, instanceOf(KillRequest.class));
            KillRequest kreq = (KillRequest) req;
            assertThat(kreq.id(), is(2));
        }
    }

    @Test
    public void decodeResponses() throws IOException {
        {
            String json = "{\"command\":\"list\", \"status\":\"ok\", \"queue\":[]}";
            Response res = mapper.readValue(json, Response.class);
            assertThat(res, instanceOf(ListJobResponse.class));
            ListJobResponse lres = (ListJobResponse) res;
            assertThat(lres.queue().size(), is(0));
        }
        {
            String json = "{\"command\":\"kill\", \"status\":\"ok\"}";
            Response res = mapper.readValue(json, Response.class);
            assertThat(res, instanceOf(KillResponse.class));
        }
        {
            String jobJson = "{\"appid\" : \"moo\", \"cmd\":\"ls -l\", \"id\":23}";
            String json = "{\"command\":\"schedule\", \"status\":\"ok\", \"job\":" + jobJson + "}";
            Response res = mapper.readValue(json, Response.class);
            assertThat(res, instanceOf(ScheduleResponse.class));
            ScheduleResponse sres = (ScheduleResponse) res;
            assertNotNull(sres.job);
            assertThat(sres.job.id(), is(23));
        }
        {
            String json = "{\"command\":\"get-job\",\"job\":{\"appid\":\"foobar\", \"cmd\":\"Mmmmmmmmmy commmmmand1!!!!!\",\"scheduled\":null,\"started\":null,\"finished\":null,\"result\":0,\"id\":0,\"url\":null}}";

            Response res = mapper.readValue(json, Response.class);
            assertThat(res, instanceOf(GetJobResponse.class));
            GetJobResponse getJobResponse = (GetJobResponse)res;
            assertThat(getJobResponse.job().get().id(), is(0));
            assertThat(getJobResponse.job().get().cmd(), is("Mmmmmmmmmy commmmmand1!!!!!"));
        }
        {
            String json = "{\"command\":\"watch\", \"status\":\"ok\", \"event\":\"finished\", \"job\":{\"cmd\":\"ls\", \"id\":23, \"appid\":\"myapp\"}}";
            Response res = mapper.readValue(json, Response.class);
            assertThat(res, instanceOf(WatchResponse.class));
            WatchResponse wres = (WatchResponse) res;
            assertThat(wres.status(), is("ok"));
            assertThat(wres.event(), is("finished"));
            assertNotNull(wres.job());
        }
        {
            String json = "{\"command\":\"status\",\"queueLength\":1,\"runningLength\":0,\"numSlaves\":0,\"watcherLength\":0,\"sessionLength\":1,\"status\":\"ok\"}";
            Response res = mapper.readValue(json, Response.class);
            assertThat(res, instanceOf(StatusResponse.class));
            StatusResponse statusResponse = (StatusResponse) res;
            assertThat(statusResponse.sessionLength(), is(1));
            assertThat(statusResponse.queueLength(), is(1));
        }
    }

    @Test
    public void encode() throws JsonProcessingException, IOException {
        {
            ListJobRequest req = new ListJobRequest(0);
            String json = mapper.writeValueAsString(req);
            Request req2 = mapper.readValue(json, Request.class);
            assertThat(req2, instanceOf(ListJobRequest.class));
        }

        {
            Job job = new Job("foobar-app", "ls -l", null, new Range(1, 0), new Range(128, 0));
            ScheduleRequest scheduleRequest = new ScheduleRequest(job, false);
            String json = mapper.writeValueAsString(scheduleRequest);
            Request req = mapper.readValue(json, Request.class);
            assertThat(req, instanceOf(ScheduleRequest.class));
            ScheduleRequest scheduleRequest1 = (ScheduleRequest) req;
            assertNotNull(scheduleRequest1.job());

            GetJobResponse getJobResponse = new GetJobResponse(Optional.of(job));
            mapper.writeValueAsString(getJobResponse);
        }
        // Hanc marginis exiguitas non caperet.
    }

    @Test
    public void timeout() {
        assertThat(Connection.IDLE_TIMEOUT_SEC, greaterThan(Connection.KEEPALIVE_INTERVAL_SEC));
    }
}
