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
package io.github.retz.web;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import io.github.retz.auth.Authenticator;
import io.github.retz.cli.FileConfiguration;
import io.github.retz.cli.TimestampHelper;
import io.github.retz.protocol.*;
import io.github.retz.protocol.data.Application;
import io.github.retz.protocol.data.Job;
import io.github.retz.scheduler.Applications;
import io.github.retz.scheduler.JobQueue;
import io.github.retz.scheduler.RetzScheduler;
import org.apache.mesos.Protos;
import org.apache.mesos.SchedulerDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Request;
import spark.Response;
import spark.Spark;

import java.net.URI;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static spark.Spark.*;


public final class WebConsole {
    private static final Logger LOG = LoggerFactory.getLogger(WebConsole.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static Optional<RetzScheduler> scheduler = Optional.empty();
    private static Optional<SchedulerDriver> driver = Optional.empty();

    private Thread clientMonitorThread;
    private ClientMonitor clientMonitor;

    static {
        MAPPER.registerModule(new Jdk8Module());
    }

    public WebConsole(FileConfiguration config) {

        if (config.isTLS()) {
            LOG.info("HTTPS enabled. Keystore file={}, keystore pass={} chars, Truststore file={}, Truststorepass={} chars",
                    config.getKeystoreFile(), config.getKeystorePass().length(),
                    config.getTruststoreFile(), "(not printed)");
            secure(config.getKeystoreFile(), config.getKeystorePass(), config.getTruststoreFile(), config.getTruststorePass());
        } else {
            LOG.info("HTTPS disabled. Scheme: {}", config.getUri().getScheme());
        }
        port(config.getUri().getPort());
        staticFileLocation("/public");

        // APIs to be in WebSocket: watch, run
        webSocket("/cui", ConsoleWebSocketHandler.class);
        webSocketIdleTimeoutMillis(Connection.IDLE_TIMEOUT_SEC * 1000);

        before((req, res) -> {
            // TODO: authenticator must be per each user and single admin user
            Optional<Authenticator> authenticator = Optional.ofNullable(config.getAuthenticator());
            String verb = req.requestMethod();
            String md5 = req.headers("content-md5");
            if (md5 == null) {
                md5 = "";
            }
            String date = req.headers("date");
            String resource;

            if (req.raw().getQueryString() != null) {
                resource = new StringBuilder().append(new URI(req.url()).getPath())
                        .append("?").append(req.raw().getQueryString()).toString();
            } else {
                resource = new URI(req.url()).getPath();
            }

            LOG.debug("req={}, res={}, resource=", req, res, resource);
            // These don't require authentication to simplify operation
            if ("/ping".equals(resource) || "/status".equals(resource)
                    || "/cui".equals(resource) // TODO: this is special exception; in future this must be removed...
                    || resource.equals(RetzScheduler.getJarPath())) {
                return;
            }

            LOG.info("{} {} from {} {}", req.requestMethod(), resource, req.ip(), req.userAgent());

            String givenSignature =req.headers(Authenticator.AUTHORIZATION);
            LOG.debug("Signature from client: {}", givenSignature);

            if (authenticator.isPresent()) {
                Optional<Authenticator.AuthHeaderValue> authHeaderValue = Authenticator.parseHeaderValue(givenSignature);
                if (!authHeaderValue.isPresent()) {
                    halt(401, "Bad Authorization header: " + givenSignature);
                }

                if (!authenticator.get().authenticate(verb, md5, date, resource,
                        authHeaderValue.get().key(), authHeaderValue.get().signature())) {
                    String string2sign = authenticator.get().string2sign(verb, md5, date, resource);
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Auth failed. Calculated signature={}, Given signature={}",
                                authenticator.get().signature(verb, md5, date, resource),
                                authHeaderValue.get().signature());
                    }
                    halt(401, "Authentication failed. String to sign: " + string2sign);
                }
            }
        });

        // APIs to be in vanilla HTTP
        get("/ping", (req, res) -> "OK");
        get("/status", WebConsole::status);

        // /jobs GET -> list
        get(ListJobRequest.resourcePattern(), (req, res) -> {
            LOG.debug("list jobs");
            //io.github.retz.protocol.Request request = MAPPER.readValue(req.bodyAsBytes(), io.github.retz.protocol.Request.class);
            ListJobRequest listJobRequest = new ListJobRequest(64);
            ListJobResponse listJobResponse = WebConsole.list(listJobRequest.limit());
            listJobResponse.ok();
            res.status(200);
            res.type("application/json");
            return MAPPER.writeValueAsString(listJobResponse);
        });
        // /job  PUT -> schedule, GET -> get-job, DELETE -> kill
        get(GetJobRequest.resourcePattern(), JobRequestRouter::getJob);
        // Get a file
        get(GetFileRequest.resourcePattern(), JobRequestRouter::getFile);
        // Get file list
        get(ListFilesRequest.resourcePattern(), JobRequestRouter::getPath);

        put(ScheduleRequest.resourcePattern(), (req, res) -> {
            ScheduleRequest scheduleRequest = MAPPER.readValue(req.bodyAsBytes(), ScheduleRequest.class);
            res.type("application/json");
            if (Applications.get(scheduleRequest.job().appid()).isPresent()) {
                Job job = scheduleRequest.job();
                job.schedule(JobQueue.issueJobId(), TimestampHelper.now());

                JobQueue.push(job);

                ScheduleResponse scheduleResponse = new ScheduleResponse(job);
                scheduleResponse.ok();
                LOG.info("Job '{}' at {} has been scheduled at {}.", job.cmd(), job.appid(), job.scheduled());

                res.status(201);
                return MAPPER.writeValueAsString(scheduleResponse);

            } else {
                LOG.warn("No such application loaded: {}", scheduleRequest.job().appid());
                ErrorResponse response = new ErrorResponse("No such application: " + scheduleRequest.job().appid());
                res.status(404);
                return MAPPER.writeValueAsString(response);
            }
        });

        delete(KillRequest.resourcePattern(), (req, res) -> {
            LOG.info("kill", req.params(":id"));
            int id = Integer.parseInt(req.params(":id")); // or 400 when failed?
            WebConsole.kill(id);
            res.status(200);
            KillResponse response = new KillResponse();
            response.ok();
            return MAPPER.writeValueAsString(response);
        });

        // /apps GET -> list-app
        get(ListAppRequest.resourcePattern(), (req, res) -> {
            ListAppResponse response = new ListAppResponse(WebConsole.listApps());
            response.ok();
            return MAPPER.writeValueAsString(response);
        });

        // /app  PUT -> load, GET -> get-app, DELETE -> unload-app
        put(LoadAppRequest.resourcePattern(), (req, res) -> {
            LOG.debug(LoadAppRequest.resourcePattern());

            LoadAppRequest loadAppRequest = MAPPER.readValue(req.bodyAsBytes(), LoadAppRequest.class);
            LOG.debug("app id={}", loadAppRequest.application().getAppid());
            boolean result = WebConsole.load(loadAppRequest.application());

            if (result) {
                res.status(200);
                LoadAppResponse response = new LoadAppResponse();
                response.ok();
                return MAPPER.writeValueAsString(response);
            } else {
                res.status(400);
                return MAPPER.writeValueAsString(new ErrorResponse("cannot load application"));
            }
        });

        get(GetAppRequest.resourcePattern(), (req, res) -> {
            String appname = req.params(":name");
            LOG.debug("deleting app {}", appname);
            Optional<Application> maybeApp = Applications.get(appname);
            res.type("application/json");
            if (maybeApp.isPresent()) {
                res.status(200);
                GetAppResponse getAppResponse = new GetAppResponse(maybeApp.get());
                getAppResponse.ok();
                return MAPPER.writeValueAsString(getAppResponse);
            } else {
                ErrorResponse response = new ErrorResponse("No such application: " + appname);
                res.status(404);
                return MAPPER.writeValueAsString(response);
            }
        });

        delete(UnloadAppRequest.resourcePattern(), (req, res) -> {
            String appname = req.params(":name");
            LOG.info("deleting app {}", appname);
            WebConsole.unload(appname);
            UnloadAppResponse response = new UnloadAppResponse();
            response.ok();
            return MAPPER.writeValueAsString(response);
        });

        clientMonitor = new ClientMonitor(Connection.KEEPALIVE_INTERVAL_SEC);  // I won't make this configurable until it's needed
        clientMonitorThread = new Thread(clientMonitor);
        clientMonitorThread.setName("ClientMonitor");
        clientMonitorThread.start();

        init();
    }

    public static void setScheduler(RetzScheduler scheduler) {
        WebConsole.scheduler = Optional.ofNullable(scheduler);
    }

    public static void setDriver(SchedulerDriver driver) {
        WebConsole.driver = Optional.ofNullable(driver);
    }

    public static String status(Request request, Response response) {
        io.github.retz.protocol.Response res;
        if (scheduler.isPresent()) {
            StatusResponse statusResponse = new StatusResponse();
            JobQueue.setStatus(statusResponse);
            ConsoleWebSocketHandler.setStatus(statusResponse);
            res = statusResponse;
        } else {
            res = new ErrorResponse("no scheduler set now");
        }
        try {
            return MAPPER.writeValueAsString(res);
        } catch (JsonProcessingException e) {
            // TODO: how can we return 503?
            return "fail";
        }
    }

    public void stop() {
        clientMonitor.stop();
        try {
            clientMonitorThread.join();
        } catch (InterruptedException e) {
            LOG.warn("Can't join client monitor thread: " + e.toString());
        }
        Spark.stop();
    }

    public static ListJobResponse list(int limit) {
        List<Job> queue = JobQueue.getAll();
        List<Job> running = new LinkedList<>();
        for (Map.Entry<String, Job> entry : JobQueue.getRunning().entrySet()) {
            running.add(entry.getValue());
        }
        List<Job> finished = new LinkedList<>();
        JobQueue.getAllFinished(finished, limit);

        return new ListJobResponse(queue, running, finished);
    }

    public static boolean kill(int id) {
        if (! driver.isPresent()) {
            LOG.error("Driver is not present; this setup should be wrong");
            return false;
        }
        Optional<Job> maybeJob = JobQueue.cancel(id);
        if (maybeJob.isPresent()) {
            LOG.info("Job id={} was in the queue and canceled.", id);
            notifyKilled(maybeJob.get());
            maybeJob.get().killed(TimestampHelper.now(), "Canceled by user");
            JobQueue.finished(maybeJob.get());
            return true;
        }
        // There's a slight pitfall between cancel above and kill below where
        // no kill may be sent, RetzScheduler is exactly in resourceOffers and being scheduled.
        // Then this protocol returns false for sure.
        for (Map.Entry<String, Job> e : JobQueue.getRunning().entrySet()) {
            if (e.getValue().id() == id) {
                Protos.TaskID taskId = Protos.TaskID.newBuilder().setValue(e.getKey()).build();
                Protos.Status status = driver.get().killTask(taskId);
                LOG.info("Job id={} was running and killed.");
                return status == Protos.Status.DRIVER_RUNNING;
            }
        }
        return false;
    }

    public static boolean load(Application app) {
        return Applications.load(app);
    }

    public static List<Application> listApps() {
        return Applications.getAll();
    }

    public static void unload(String appName) {
        Applications.unload(appName);
        LOG.info("Unloaded {}", appName);
        if (WebConsole.driver.isPresent() &&
                WebConsole.scheduler.isPresent()) {
            WebConsole.scheduler.get().stopAllExecutors(WebConsole.driver.get(), appName);
        }
        LOG.info("Stopped all executors invoked as {}", appName);
    }

    public static void notifyStarted(Job job) {
        broadcast(EventType.STARTED, job);
        ConsoleWebSocketHandler.notify("started", job);
    }

    public static void notifyFinished(Job job) {
        broadcast(EventType.FINISHED, job);
        ConsoleWebSocketHandler.notify("finished", job);
    }

    public static void notifyKilled(Job job) {
        broadcast(EventType.KILLED, job);
        ConsoleWebSocketHandler.notify("killed", job);
    }

    // TODO: make this async with some global message queue
    private static void broadcast(EventType eventType, Job job) {
        switch (eventType) {
            case STARTED:
                ConsoleWebSocketHandler.broadcast("started", job, "ok");
                break;
            case SCHEDULED:
                ConsoleWebSocketHandler.broadcast("scheduled", job, "ok");
                break;
            case KILLED:
                ConsoleWebSocketHandler.broadcast("killed", job, "killed");
                break;
            case FINISHED:
                ConsoleWebSocketHandler.broadcast("finished", job, "ok");
                break;
            default:
                throw new AssertionError("Unknown EventType: " + eventType.toString());
        }
    }

    private enum EventType {
        STARTED,
        SCHEDULED,
        KILLED,
        FINISHED
    }

}
