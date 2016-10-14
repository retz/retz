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
import io.github.retz.protocol.data.DockerContainer;
import io.github.retz.protocol.data.Job;
import io.github.retz.protocol.data.User;
import io.github.retz.scheduler.Applications;
import io.github.retz.scheduler.Database;
import io.github.retz.scheduler.JobQueue;
import io.github.retz.scheduler.RetzScheduler;
import org.apache.mesos.Protos;
import org.apache.mesos.SchedulerDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Request;
import spark.Response;
import spark.Spark;

import java.io.IOException;
import java.net.URI;
import java.util.LinkedList;
import java.util.List;
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
            res.header("Server", RetzScheduler.HTTP_SERVER_NAME);

            String resource;

            if (req.raw().getQueryString() != null) {
                resource = new StringBuilder().append(new URI(req.url()).getPath())
                        .append("?").append(req.raw().getQueryString()).toString();
            } else {
                resource = new URI(req.url()).getPath();
            }
            LOG.info("{} {} from {} {}", req.requestMethod(), resource, req.ip(), req.userAgent());

            // TODO: authenticator must be per each user and single admin user
            Optional<Authenticator> adminAuthenticator = Optional.ofNullable(config.getAuthenticator());
            if (!adminAuthenticator.isPresent()) {
                // No authentication required
                return;
            }

            String verb = req.requestMethod();
            String md5 = req.headers("content-md5");
            if (md5 == null) {
                md5 = "";
            }
            String date = req.headers("date");

            LOG.debug("req={}, res={}, resource=", req, res, resource);
            // These don't require authentication to simplify operation
            if ("/ping".equals(resource) || "/status".equals(resource)
                    || "/cui".equals(resource) // TODO: this is special exception; in future this must be removed...
                    || "/".equals(resource)) {
                return;
            }

            Optional<Authenticator.AuthHeaderValue> authHeaderValue = getAuthInfo(req);
            if (!authHeaderValue.isPresent()) {
                halt(401, "Bad Authorization header: " + req.headers(Authenticator.AUTHORIZATION));
            }

            Authenticator authenticator;
            if (adminAuthenticator.get().getKey().equals(authHeaderValue.get().key())) {
                // Admin
                authenticator = adminAuthenticator.get();
            } else {
                // Not admin
                Optional<User> u = Database.getUser(authHeaderValue.get().key());
                if (!u.isPresent()) {
                    halt(403, "No such user");
                }
                authenticator = new Authenticator(u.get().keyId(), u.get().secret());
            }

            if (!authenticator.authenticate(verb, md5, date, resource,
                    authHeaderValue.get().key(), authHeaderValue.get().signature())) {
                String string2sign = authenticator.string2sign(verb, md5, date, resource);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Auth failed. Calculated signature={}, Given signature={}",
                            authenticator.signature(verb, md5, date, resource),
                            authHeaderValue.get().signature());
                }
                halt(401, "Authentication failed. String to sign: " + string2sign);
            }
        });

        after((req, res) -> {
            LOG.debug("{} {} {} {} from {} {}",
                    res.raw().getStatus(),
                    req.requestMethod(), req.url(), req.raw().getQueryString(), req.ip(), req.userAgent());
        });

        // APIs to be in vanilla HTTP
        get("/ping", (req, res) -> "OK");
        get("/status", WebConsole::status);

        // TODO: XXX: validate application owner at ALL job-related APIs
        // /jobs GET -> list
        get(ListJobRequest.resourcePattern(), (req, res) -> {
            Optional<Authenticator.AuthHeaderValue> authHeaderValue = getAuthInfo(req);
            LOG.debug("list jobs owned by {}", authHeaderValue.get().key());

            ListJobResponse listJobResponse = WebConsole.list(authHeaderValue.get().key(), -1);
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

        put(ScheduleRequest.resourcePattern(), WebConsole::schedule);

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
            Optional<Authenticator.AuthHeaderValue> authHeaderValue = getAuthInfo(req);
            LOG.info("Listing all apps owned by {}", authHeaderValue.get().key());
            ListAppResponse response = new ListAppResponse(Applications.getAll(authHeaderValue.get().key()));
            response.ok();
            return MAPPER.writeValueAsString(response);
        });

        // /app  PUT -> load, GET -> get-app, DELETE -> unload-app
        put(LoadAppRequest.resourcePattern(), (req, res) -> {
            LOG.debug(LoadAppRequest.resourcePattern());
            Optional<Authenticator.AuthHeaderValue> authHeaderValue = getAuthInfo(req);
            res.type("application/json");

            // TODO: check key from Authorization header matches a key in Application object
            LoadAppRequest loadAppRequest = MAPPER.readValue(req.bodyAsBytes(), LoadAppRequest.class);
            LOG.debug("app (id={}, owner={}), requested by {}",
                    loadAppRequest.application().getAppid(), loadAppRequest.application().getOwner(),
                    authHeaderValue.get().key());

            // Compare application owner and requester
            validateOwner(req, loadAppRequest.application());

            if (!(loadAppRequest.application().container() instanceof DockerContainer)) {
                if (loadAppRequest.application().getUser().isPresent() &&
                        loadAppRequest.application().getUser().get().equals("root")) {
                    res.status(400);
                    return MAPPER.writeValueAsString(new ErrorResponse("root user is only allowed with Docker container"));
                }
            }

            Application app = loadAppRequest.application();
            LOG.info("Registering application name={} owner={}", app.getAppid(), app.getOwner());
            boolean result = Applications.load(app);

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
            LOG.debug(LoadAppRequest.resourcePattern());
            Optional<Authenticator.AuthHeaderValue> authHeaderValue = getAuthInfo(req);

            String appname = req.params(":name");
            LOG.debug("deleting app {} requested by {}", appname, authHeaderValue.get().key());
            Optional<Application> maybeApp = Applications.get(appname);
            res.type("application/json");
            if (maybeApp.isPresent()) {
                // Compare application owner and requester
                validateOwner(req, maybeApp.get());

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
            LOG.warn("deleting app {} (This API is deprecated)", appname);
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
            scheduler.get().setOfferStats(statusResponse);
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

    static Optional<Authenticator.AuthHeaderValue>  getAuthInfo(Request req) {
        String givenSignature = req.headers(Authenticator.AUTHORIZATION);
        LOG.debug("Signature from client: {}", givenSignature);

        return Authenticator.parseHeaderValue(givenSignature);
    }

    static void validateOwner(Request req, Application app) {
        Optional<Authenticator.AuthHeaderValue> authHeaderValue = getAuthInfo(req);
        if (!app.getOwner().equals(authHeaderValue.get().key())) {
            LOG.debug("Invalid request: requester and owner does not match");
            halt(400, "Invalid request: requester and owner does not match");
        }
    }

    public static ListJobResponse list(String id, int limit) {
        List<Job> queue = new LinkedList<>(); //JobQueue.getAll();
        List<Job> running = new LinkedList<>();
        List<Job> finished = new LinkedList<>();

        for (Job job : JobQueue.getAll(id)) {
            switch (job.state()) {
                case QUEUED:
                    queue.add(job);
                    break;
                case STARTING:
                case STARTED:
                    running.add(job);
                    break;
                case FINISHED:
                case KILLED:
                    finished.add(job);
                    break;
                default:
                    LOG.error("Cannot be here: id={}, state={}", job.id(), job.state());
            }
        }
        return new ListJobResponse(queue, running, finished);
    }

    public static boolean kill(int id) {
        if (!driver.isPresent()) {
            LOG.error("Driver is not present; this setup should be wrong");
            return false;
        }

        // TODO: non-application owner is even possible to kill job
        Optional<String> maybeTaskId = JobQueue.cancel(id, "Canceled by user");

        // There's a slight pitfall between cancel above and kill below where
        // no kill may be sent, RetzScheduler is exactly in resourceOffers and being scheduled.
        // Then this protocol returns false for sure.
        if (maybeTaskId.isPresent()) {
            Protos.TaskID taskId = Protos.TaskID.newBuilder().setValue(maybeTaskId.get()).build();
            Protos.Status status = driver.get().killTask(taskId);
            LOG.info("Job id={} was running and killed.");
            return status == Protos.Status.DRIVER_RUNNING;
        }
        return false;
    }

    @Deprecated
    public static void unload(String appName) {
        // TODO: non-application owner is even possible to kill job
        Applications.unload(appName);
        LOG.info("Unloaded {}", appName);
        if (WebConsole.driver.isPresent() &&
                WebConsole.scheduler.isPresent()) {
            WebConsole.scheduler.get().stopAllExecutors(WebConsole.driver.get(), appName);
        }
        LOG.info("Stopped all executors invoked as {}", appName);
    }

    public static String schedule(Request req, Response res) throws IOException, InterruptedException {
        ScheduleRequest scheduleRequest = MAPPER.readValue(req.bodyAsBytes(), ScheduleRequest.class);
        res.type("application/json");
        Optional<Application> maybeApp = Applications.get(scheduleRequest.job().appid()); // TODO check owner right here
        if (!maybeApp.isPresent()) {
            LOG.warn("No such application loaded: {}", scheduleRequest.job().appid());
            ErrorResponse response = new ErrorResponse("No such application: " + scheduleRequest.job().appid());
            res.status(404);
            return MAPPER.writeValueAsString(response);

        } else if (maybeApp.get().enabled()) {

            validateOwner(req, maybeApp.get());

            Job job = scheduleRequest.job();
            job.schedule(JobQueue.issueJobId(), TimestampHelper.now());

            JobQueue.push(job);
            if (scheduler.isPresent() && driver.isPresent()) {
                LOG.info("Trying invocation from offer stock");
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

