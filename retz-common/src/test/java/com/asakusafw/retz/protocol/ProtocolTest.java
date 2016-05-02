/**
 *    Retz
 *    Copyright (C) 2016 Nautilus Technologies, KK.
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
package com.asakusafw.retz.protocol;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

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
            assert job.cmd().equals("yaess/bin/yaess-batch.sh ....");
            assert job.result() == 0;
            assert job.id() == 1;
        }
        {
            String jobString = "{ \"appid\" : \"myid\", \"cmd\":\"yaess/bin/yaess-batch.sh ....\", \"scheduled\":\"2016-05-15:20:20:20Z\", \"id\":2}";
            Job job = mapper.readValue(jobString, Job.class);
            assert job.cmd().equals("yaess/bin/yaess-batch.sh ....");
            assert job.started() == null;
            assert job.result() == 0;
        }
        {
            String appString = "{\"appid\":\"my-one-of-42-apps\", \"files\":[\"http://example.com/app.tar.gz\"]}";
            Application app = mapper.readValue(appString, Application.class);
            assert app.getAppid().equals("my-one-of-42-apps");
            assert app.getFiles() != null;
            assert app.getFiles().size() == 1;
            assert !app.getDiskMB().isPresent();
        }
        {
            String appString = "{\"appid\":\"foobar\",\"files\":[\"http://example.com:234/foobar/test.tar.gz\"], \"diskMB\":null}";
            Application app = mapper.readValue(appString, Application.class);
            assert !app.getDiskMB().isPresent();
        }
        {
            String metajobString = "{\"job\":{\"cmd\":\"touch /tmp/junit3807420247585460493.tmp\",\"scheduled\":null,\"started\":null,\"finished\":null,\"result\":-1,\"id\":0,\"url\":null,\"appid\":\"appname\",\"name\":null,\"cpu\":{\"min\":1,\"max\":2147483647},\"memMB\":{\"min\":128,\"max\":2147483647},\"props\":null},\"app\":{\"appid\":\"appname\",\"persistentFiles\":[],\"files\":[],\"diskMB\":null}}";
            MetaJob metaJob = mapper.readValue(metajobString, MetaJob.class);
            assert metaJob.getApp() != null;
        }
    }

    public void decodeRequests() throws IOException {

        {
            String json = "{\"command\":\"list\"}";
            Request req = mapper.readValue(json, Request.class);
            assert req instanceof ListJobRequest;
        }
        {
            String json = "{\"command\":\"schedule\",\"job\":{\"cmd\":\"ls -l\"}}";
            Request req = mapper.readValue(json, Request.class);
            assert req instanceof ScheduleRequest;
            ScheduleRequest sreq = (ScheduleRequest) req;
            assert sreq.job() != null;
            assert sreq.job().cmd().equals("ls -l");
        }
        {
            String json = "{\"command\":\"schedule\",\"job\":{\"cmd\":\"Mmmmmmmmmy commmmmand1!!!!!\",\"scheduled\":null,\"started\":null,\"finished\":null,\"result\":0,\"id\":0,\"url\":null}}";
            Request req = mapper.readValue(json, Request.class);
            assert req instanceof ScheduleRequest;
            ScheduleRequest sreq = (ScheduleRequest) req;
            assert sreq.job() != null;
            assert sreq.job().cmd().equals("Mmmmmmmmmy commmmmand1!!!!!");
        }
        {
            String json = "{\"command\":\"kill\",\"id\":2}}";
            Request req = mapper.readValue(json, Request.class);
            assert req instanceof KillRequest;
            KillRequest kreq = (KillRequest) req;
            assert kreq.id() == 2;
        }
        {
            String json = "{\"command\":\"watch\"}";
            Request req = mapper.readValue(json, Request.class);
            assert req instanceof WatchRequest;
        }
    }

    @Test
    public void decodeResponses() throws IOException {
        {
            String json = "{\"command\":\"list\", \"status\":\"ok\", \"queue\":[]}";
            Response res = mapper.readValue(json, Response.class);
            assert res instanceof ListJobResponse;
            ListJobResponse lres = (ListJobResponse) res;
            assert lres.queue().size() == 0;
        }
        {
            String json = "{\"command\":\"kill\", \"status\":\"ok\"}";
            Response res = mapper.readValue(json, Response.class);
            assert res instanceof KillResponse;
        }
        {
            String jobJson = "{\"appid\" : \"moo\", \"cmd\":\"ls -l\", \"id\":23}";
            String json = "{\"command\":\"schedule\", \"status\":\"ok\", \"job\":" + jobJson + "}";
            Response res = mapper.readValue(json, Response.class);
            assert res instanceof ScheduleResponse;
            ScheduleResponse sres = (ScheduleResponse) res;
            assert sres.job != null;
            assert sres.job.id() == 23;
        }
        {
            String json = "{\"command\":\"watch\", \"status\":\"ok\", \"event\":\"finished\", \"job\":{\"cmd\":\"ls\", \"id\":23, \"appid\":\"myapp\"}}";
            Response res = mapper.readValue(json, Response.class);
            assert res instanceof WatchResponse;
            WatchResponse wres = (WatchResponse) res;
            assert wres.status().equals("ok");
            assert wres.event().equals("finished");
            assert wres.job() != null;
        }
        {
            String json = "{\"command\":\"status\",\"queueLength\":1,\"runningLength\":0,\"numSlaves\":0,\"watcherLength\":0,\"sessionLength\":1,\"status\":\"ok\"}";
            Response res = mapper.readValue(json, Response.class);
            assert res instanceof StatusResponse;
            StatusResponse statusResponse = (StatusResponse) res;
            assert statusResponse.sessionLength() == 1;
            assert statusResponse.queueLength() == 1;
        }
    }

    @Test
    public void encode() throws JsonProcessingException, IOException {
        {
            ListJobRequest req = new ListJobRequest();
            String json = mapper.writeValueAsString(req);
            Request req2 = mapper.readValue(json, Request.class);
            assert req2 instanceof ListJobRequest;
        }

        {
            Job job = new Job("foobar-app", "ls -l", null, new Range(1, 0), new Range(128, 0));
            ScheduleRequest scheduleRequest = new ScheduleRequest(job, false);
            String json = mapper.writeValueAsString(scheduleRequest);
            Request req = mapper.readValue(json, Request.class);
            assert req instanceof ScheduleRequest;
            ScheduleRequest scheduleRequest1 = (ScheduleRequest) req;
            assert scheduleRequest1.job() != null;
        }
        // Hanc marginis exiguitas non caperet.
    }
}
