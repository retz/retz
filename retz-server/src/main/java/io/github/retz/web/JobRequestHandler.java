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
package io.github.retz.web;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import io.github.retz.auth.AuthHeader;
import io.github.retz.cli.TimestampHelper;
import io.github.retz.db.Database;
import io.github.retz.mesosc.MesosHTTPFetcher;
import io.github.retz.misc.Pair;
import io.github.retz.planner.AppJobPair;
import io.github.retz.protocol.*;
import io.github.retz.protocol.data.Application;
import io.github.retz.protocol.data.DirEntry;
import io.github.retz.protocol.data.FileContent;
import io.github.retz.protocol.data.Job;
import io.github.retz.scheduler.*;
import org.apache.commons.io.IOUtils;
import org.apache.mesos.Protos;
import org.apache.mesos.SchedulerDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Request;

import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static io.github.retz.web.WebConsole.validateOwner;
import static spark.Spark.halt;

public class JobRequestHandler {
    private static final Logger LOG = LoggerFactory.getLogger(JobRequestHandler.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static Optional<RetzScheduler> scheduler = Optional.empty();
    private static Optional<SchedulerDriver> driver = Optional.empty();
    private static int MAX_LIST_JOB_SIZE = Integer.MAX_VALUE;

    static {
        MAPPER.registerModule(new Jdk8Module());
    }

    static void setScheduler(RetzScheduler sched) {
        scheduler = Optional.ofNullable(sched);
    }

    static void setDriver(SchedulerDriver d) {
        driver = Optional.ofNullable(d);
    }

    static void setMaxListJobSize(int v) {
        LOG.info("Setting max list-job size as {}", v);
        MAX_LIST_JOB_SIZE = v;
    }

    static String listJob(spark.Request req, spark.Response res) throws IOException {
        Optional<AuthHeader> authHeaderValue = WebConsole.getAuthInfo(req);
        LOG.debug("list jobs owned by {}", authHeaderValue.get().key());
        ListJobRequest listJobRequest = MAPPER.readValue(req.body(), ListJobRequest.class);
        LOG.debug("q: state={}, tag={}",
                listJobRequest.state(), listJobRequest.tag());
        String user = Objects.requireNonNull(authHeaderValue.get().key());
        try {
            List<Job> jobs = JobQueue.list(user, listJobRequest.state(), listJobRequest.tag(), MAX_LIST_JOB_SIZE);

            boolean more = false;
            if (jobs.size() > ListJobResponse.MAX_JOB_NUMBER) {
                more = true;
                jobs = jobs.subList(0, ListJobResponse.MAX_JOB_NUMBER);
            }
            ListJobResponse listJobResponse = new ListJobResponse(jobs, more);
            listJobResponse.ok();
            res.status(200);
            res.type("application/json");
            return MAPPER.writeValueAsString(listJobResponse);
        } catch (SQLException e) {
            LOG.error(e.toString(), e);
            res.status(500);
            return "\"Internal Error\"";
        }
    }

    private static Optional<Job> getJobAndVerify(Request req) throws IOException {
        int id = Integer.parseInt(req.params(":id"));
        Optional<AuthHeader> authHeaderValue = WebConsole.getAuthInfo(req);

        if (!authHeaderValue.isPresent()) {
            LOG.debug("Authorization header lacking?");
            return Optional.empty();
        }
        LOG.debug("get-xxx id={}, user={}", id, authHeaderValue.get().key());

        Optional<AppJobPair> maybePair = Database.getInstance().getAppJob(id);
        if (maybePair.isPresent()) {
            AppJobPair pair = maybePair.get();
            if (pair.application().getOwner().equals(authHeaderValue.get().key())) {
                return Optional.of(pair.job());
            }
        }
        return Optional.empty();
    }

    static String getJob(spark.Request req, spark.Response res) throws IOException {
        Optional<Job> maybeJob = getJobAndVerify(req);

        res.type("application/json");

        Response response = new GetJobResponse(maybeJob);
        response.status("ok");
        res.status(200);
        return MAPPER.writeValueAsString(response);
    }

    static String getFile(spark.Request req, spark.Response res) throws IOException {
        Optional<Job> job = getJobAndVerify(req);

        String file = req.queryParams("path");
        long offset = Long.parseLong(req.queryParams("offset"));
        long length = Long.parseLong(req.queryParams("length"));

        LOG.debug("get-file: path={}, offset={}, length={}", file, offset, length);
        res.type("application/json");

        Optional<FileContent> fileContent;
        if (job.isPresent() && job.get().url() != null // If url() is null, the job hasn't yet been started at Mesos
                && MesosHTTPFetcher.statHTTPFile(job.get().url(), file)) {
            Pair<Integer, String> payload = MesosHTTPFetcher.fetchHTTPFile(job.get().url(), file, offset, length);
            LOG.debug("Payload length={}, offset={}", payload.right().length(), offset);
            // TODO: what the heck happens when a file is not UTF-8 encodable???? How Mesos works?
            if (payload.left() == 200) {
                fileContent = Optional.ofNullable(MAPPER.readValue(payload.right(), FileContent.class));
            } else {
                return MAPPER.writeValueAsString(new ErrorResponse(payload.right()));
            }
        } else {
            fileContent = Optional.empty();
        }
        GetFileResponse getFileResponse = new GetFileResponse(job, fileContent);
        getFileResponse.ok();
        res.status(200);

        return MAPPER.writeValueAsString(getFileResponse);
    }

    // A new HTTP endpoint to support binaries
    static String downloadFile(spark.Request req, spark.Response res) throws IOException {
        Optional<Job> job = getJobAndVerify(req);
        String file = req.queryParams("path");
        LOG.debug("download: path={}", file);

        if (job.isPresent() && job.get().url() != null){ // If url() is null, the job hasn't yet been started at Mesos

            // TODO: This is soo inefficient; it stores all data into RAM without streaming
            // THIS MUST BE FIXED SOON
            Pair<Integer, byte[]> payload = MesosHTTPFetcher.downloadHTTPFile(job.get().url(), file);
            res.status(payload.left());

            if (payload.left() == 200) {
                res.type("application/octet-stream");
                res.raw().setContentLength(payload.right().length);
                ByteArrayInputStream in = new ByteArrayInputStream(payload.right());

                IOUtils.copy(in, res.raw().getOutputStream());
                in.close();
            }
            return "";

        } else {
            res.status(404);
            return "";
        }
    }

    static String getDir(spark.Request req, spark.Response res) throws JsonProcessingException {
        Optional<Job> job;
        try {
            job = getJobAndVerify(req);
        } catch (IOException e) {
            return MAPPER.writeValueAsString(new ErrorResponse(e.toString()));
        }

        String path = req.queryParams("path");
        LOG.debug("get-path: path={}", path);
        res.type("application/json");

        // Translating default as SparkJava's router doesn't route '.' or empty string
        if (ListFilesRequest.DEFAULT_SANDBOX_PATH.equals(path)) {
            path = "";
        }

        List ret;
        if (job.isPresent() && job.get().url() != null) {
            try {
                Pair<Integer,String> maybeJson = MesosHTTPFetcher.fetchHTTPDir(job.get().url(), path);
                if (maybeJson.left() == 200) {
                    ret = MAPPER.readValue(maybeJson.right(), new TypeReference<List<DirEntry>>() {
                    });
                } else {
                    return MAPPER.writeValueAsString(new ErrorResponse(path + ":" + maybeJson.left() + " " + maybeJson.right()));
                }
            } catch (FileNotFoundException e) {
                res.status(404);
                LOG.warn("path {} not found", path);
                return MAPPER.writeValueAsString(new ErrorResponse(path + " not found"));
            } catch (IOException e) {
                return MAPPER.writeValueAsString(new ErrorResponse(e.toString()));
            }
        } else {
            ret = Arrays.asList();
        }

        ListFilesResponse listFilesResponse = new ListFilesResponse(job, ret);
        listFilesResponse.status("ok");
        return MAPPER.writeValueAsString(listFilesResponse);
    }

    static String kill(Request req, spark.Response res) throws JsonProcessingException {
        LOG.debug("kill", req.params(":id"));
        int id = Integer.parseInt(req.params(":id"));

        Optional<Job> maybeJob;
        try {
            maybeJob = getJobAndVerify(req);
        } catch (IOException e) {
            return MAPPER.writeValueAsString(new ErrorResponse(e.toString()));
        }

        if (!maybeJob.isPresent()) {
            res.status(404);
            Response response = new ErrorResponse("No such job: " + id);
            response.status("No such job: " + id);
            return MAPPER.writeValueAsString(response);
        }

        Optional<Boolean> result = Stanchion.call(() -> {
            Optional<Job> maybeJob2 = JobQueue.cancel(id, "Canceled by user");

            if (maybeJob2.isPresent()) {
                Job job = maybeJob2.get();
                // There's a slight pitfall between cancel above and kill below where
                // no kill may be sent, RetzScheduler is exactly in resourceOffers and being scheduled.
                // Then this protocol returns false for sure.
                if (job.taskId() != null && !job.taskId().isEmpty() && driver.isPresent()) {
                    Protos.TaskID taskId = Protos.TaskID.newBuilder().setValue(job.taskId()).build();
                    Protos.Status status = driver.get().killTask(taskId);
                    LOG.info("Job id={} was running and killed. status={}, taskId={}", job.id(), status, job.taskId());
                }
                return job.state() == Job.JobState.KILLED;
            }
            // Job is already finished or killed, no more running nor runnable, or something is wrong
            return false;
        });

        Response response;
        if (result.isPresent() && result.get()) {
            response = new KillResponse();
            res.status(200);
            response.ok();
        } else {
            res.status(500);
            response = new ErrorResponse();
            response.status("Can't kill job - due to unknown reason");
        }

        return MAPPER.writeValueAsString(response);
    }

    static String schedule(spark.Request req, spark.Response res) throws IOException, InterruptedException {
        ScheduleRequest scheduleRequest = MAPPER.readValue(req.bodyAsBytes(), ScheduleRequest.class);
        res.type("application/json");
        Optional<Application> maybeApp = Applications.get(scheduleRequest.job().appid()); // TODO check owner right here
        if (!maybeApp.isPresent()) {
            // TODO: this warn log cannot be written in real stable release
            LOG.warn("No such application loaded: {}", scheduleRequest.job().appid());
            ErrorResponse response = new ErrorResponse("No such application: " + scheduleRequest.job().appid());
            res.status(404);
            return MAPPER.writeValueAsString(response);

        } else if (maybeApp.get().enabled()) {

            validateOwner(req, maybeApp.get());

            Job job = scheduleRequest.job();
            if (scheduler.isPresent()) {
                if (!scheduler.get().validateJob(job)) {
                    String msg = "Job " + job.toString() + " does not fit system limit " + scheduler.get().maxJobSize();
                    // TODO: this warn log cannot be written in real stable release
                    LOG.warn(msg);
                    halt(400, msg);
                }
            }

            job.schedule(JobQueue.issueJobId(), TimestampHelper.now());

            JobQueue.push(job);
            if (scheduler.isPresent() && driver.isPresent()) {
                LOG.info("Trying invocation from offer stock: {}", job);
                scheduler.get().maybeInvokeNow(driver.get(), job);

            }

            ScheduleResponse scheduleResponse = new ScheduleResponse(job);
            scheduleResponse.ok();
            LOG.info("Job '{}' at {} has been scheduled at {}.", job.cmd(), job.appid(), job.scheduled());

            res.status(201);
            return MAPPER.writeValueAsString(scheduleResponse);

        } else {
            // Application is currently disabled
            res.status(401);
            ErrorResponse response = new ErrorResponse("Application " + maybeApp.get().getAppid() + " is disabled");
            return MAPPER.writeValueAsString(response);
        }
    }
}
