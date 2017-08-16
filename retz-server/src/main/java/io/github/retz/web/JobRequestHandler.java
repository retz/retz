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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import io.github.retz.auth.AuthHeader;
import io.github.retz.cli.TimestampHelper;
import io.github.retz.db.Database;
import io.github.retz.mesosc.MesosHTTPFetcher;
import io.github.retz.misc.Pair;
import io.github.retz.misc.Triad;
import io.github.retz.planner.AppJobPair;
import io.github.retz.protocol.*;
import io.github.retz.protocol.data.Application;
import io.github.retz.protocol.data.DirEntry;
import io.github.retz.protocol.data.FileContent;
import io.github.retz.protocol.data.Job;
import io.github.retz.protocol.exception.DownloadFileSizeExceeded;
import io.github.retz.protocol.exception.JobNotFoundException;
import io.github.retz.scheduler.*;
import org.apache.commons.io.IOUtils;
import org.apache.mesos.Protos;
import org.apache.mesos.SchedulerDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Request;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
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

    static String getFile(spark.Request req, spark.Response res) throws IOException, JobNotFoundException {
        Optional<Job> maybeJob = getJobAndVerify(req);
        if (! maybeJob.isPresent()) {
            GetFileResponse getFileResponse = new GetFileResponse(maybeJob, Optional.empty());
            getFileResponse.ok();
            return MAPPER.writeValueAsString(getFileResponse);
        }
        String file = req.queryParams("path");
        long offset = Long.parseLong(req.queryParams("offset"));
        long length = Long.parseLong(req.queryParams("length"));

        LOG.debug("get-file: path={}, offset={}, length={}", file, offset, length);
        res.type("application/json");

        // Check file existence
        Optional<FileContent> fileContent;
        Job job = maybeJob.get();
        if (job.url() == null) { // If url() is null, the job hasn't yet been started at Mesos / or Bug
            // TODO: re-fetch job.url() again, because it CAN be null in case of some race condition
            // This is caused by updating the url on taskUpdate and master change - re-fetch job.url()
            // requires slaveId saved with Job in database, or search the exact task from iterating over
            // all tasks from master:5050/tasks?length=10&offset=10 endpoint. Or maybe time for a new
            // clean Mesos client?
            // ditto to downloadFile() method.
            if (job.state() != Job.JobState.CREATED && job.state() != Job.JobState.STARTING && job.state() != Job.JobState.QUEUED) {
                LOG.error("Job (id={}) has its url null (state={})", job.id(), job.state());
                res.status(500);
                ErrorResponse response = new ErrorResponse("Cannot fetch file: Job has empty url");
                return MAPPER.writeValueAsString(response);
            }
            // The job has not yet started
            GetFileResponse getFileResponse = new GetFileResponse(maybeJob, Optional.empty());
            getFileResponse.ok();
            return MAPPER.writeValueAsString(getFileResponse);
        }
        if (! MesosHTTPFetcher.statHTTPFile(job.url(), file)) {
            // It is really confusing distinguishing 404 and 0-bytes to return considering offset and length
            GetFileResponse getFileResponse = new GetFileResponse(maybeJob, Optional.empty());
            getFileResponse.ok();
            return MAPPER.writeValueAsString(getFileResponse);
        }

        // Go download
        Pair<Integer, String> payload;
        try {
            payload = MesosHTTPFetcher.fetchHTTPFile(job.url(), file, offset, length);
        } catch (FileNotFoundException e) {
            res.status(404);
            LOG.warn("path {} not found", file);
            return MAPPER.writeValueAsString(new ErrorResponse(file + " not found"));
        }
        LOG.debug("Payload length={}, offset={}", payload.right().length(), offset);
        // If a file is not UTF-8 then --binary / downloadFile is the tool for it
        if (payload.left() == 200) {
            fileContent = Optional.ofNullable(MAPPER.readValue(payload.right(), FileContent.class));
            GetFileResponse getFileResponse = new GetFileResponse(maybeJob, fileContent);
            getFileResponse.ok();
            res.status(200);
            return MAPPER.writeValueAsString(getFileResponse);
        } else {
            LOG.error("{}", payload.left(), payload.right());
            res.status(payload.left()); // Is it right to just propagate status from Mesos?
            return MAPPER.writeValueAsString(new ErrorResponse(payload.right()));
        }
    }

    // A new HTTP endpoint to support binaries
    static String downloadFile(spark.Request req, spark.Response res) throws Exception {
        Optional<Job> maybeJob = getJobAndVerify(req);
        if (! maybeJob.isPresent()) {
            throw new JobNotFoundException(Integer.parseInt(req.params(":id")));
        }
        String file = req.queryParams("path");
        LOG.debug("download: path={}", file);

        // Check file
        Job job = maybeJob.get();
        if (job.url() == null) { // If url() is null, the job hasn't yet been started at Mesos / or Bug
            // TODO: re-fetch job.url() again, because it CAN be null in case of race condition where
            // updating the url on taskUpdate and master change.
            if (job.state() != Job.JobState.CREATED && job.state() != Job.JobState.STARTING && job.state() != Job.JobState.QUEUED) {
                LOG.error("Job (id={}) has its url null (state={})", job.id(), job.state());
                res.status(500);
                ErrorResponse response = new ErrorResponse("Cannot fetch file: Job has empty url");
                return MAPPER.writeValueAsString(response);
            }
            // The job has not yet started
            res.status(404);
            return "";
        }

        // Go download
        MesosHTTPFetcher.downloadHTTPFile(job.url(), file, (Triad<Integer, String, Pair<Long, InputStream>> triad) -> {
            Integer statusCode = triad.left();
            res.status(statusCode);
            if (statusCode == 200) {
                Long length = triad.right().left();
                InputStream io = triad.right().right();
                Long maxFileSize = scheduler.get().maxFileSize();
                if (length < 0) {
                    throw new IOException("content length is negative: " + length);
                } else if (0 <= maxFileSize && maxFileSize < length) { // negative maxFileSize indicates no limit
                    throw new DownloadFileSizeExceeded(length, maxFileSize);
                }
                res.raw().setHeader("Content-Length", length.toString());
                LOG.debug("start streaming of {} bytes for {}", length, file);
                IOUtils.copyLarge(io, res.raw().getOutputStream(), 0, length);
                LOG.debug("end streaming for {}", file);
            } else {
                res.body(triad.center());
            }
        });
        return "";
    }

    static String getDir(spark.Request req, spark.Response res) throws IOException {
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

        List<DirEntry> ret;
        if (job.isPresent() && job.get().url() != null) {
            Pair<Integer, String> maybeJson;
            try {
                maybeJson = MesosHTTPFetcher.fetchHTTPDir(job.get().url(), path);
            } catch (FileNotFoundException e) {
                res.status(404);
                LOG.warn("path {} not found", path);
                return MAPPER.writeValueAsString(new ErrorResponse(path + " not found"));
            }
            if (maybeJson.left() == 200) {
                ret = MAPPER.readValue(maybeJson.right(), new TypeReference<List<DirEntry>>() {
                });
            } else {
                return MAPPER.writeValueAsString(new ErrorResponse(path + ":" + maybeJson.left() + " " + maybeJson.right()));
            }
        } else {
            ret = Collections.emptyList();
        }

        ListFilesResponse listFilesResponse = new ListFilesResponse(job, ret);
        listFilesResponse.status("ok");
        return MAPPER.writeValueAsString(listFilesResponse);
    }

    static String kill(Request req, spark.Response res) throws IOException {
        LOG.debug("kill", req.params(":id"));
        res.type("application/json");
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

        boolean result = Stanchion.call(() -> {
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
        if (result) {
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

    static String schedule(spark.Request req, spark.Response res) throws IOException {
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
