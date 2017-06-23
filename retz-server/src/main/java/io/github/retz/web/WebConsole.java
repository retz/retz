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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import io.github.retz.auth.AuthHeader;
import io.github.retz.auth.Authenticator;
import io.github.retz.auth.HmacSHA256Authenticator;
import io.github.retz.auth.NoopAuthenticator;
import io.github.retz.db.Database;
import io.github.retz.protocol.*;
import io.github.retz.protocol.data.Application;
import io.github.retz.protocol.data.User;
import io.github.retz.protocol.exception.DownloadFileSizeExceeded;
import io.github.retz.protocol.exception.JobNotFoundException;
import io.github.retz.scheduler.RetzScheduler;
import io.github.retz.scheduler.ServerConfiguration;
import org.apache.mesos.SchedulerDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Request;
import spark.Response;
import spark.Spark;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static spark.Spark.*;


public final class WebConsole {
    private static final Logger LOG = LoggerFactory.getLogger(WebConsole.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final List<String> NO_AUTH_PAGES;
    private static ServerConfiguration config;

    private static Optional<RetzScheduler> scheduler = Optional.empty();

    static {
        MAPPER.registerModule(new Jdk8Module());
        String[] noAuthPages = {
                "/ping", "/status",
                "/", "/update.js", "/style.css", "/favicon.ico"};
        NO_AUTH_PAGES = Arrays.asList(noAuthPages);
    }

    public static void start(ServerConfiguration config) {

        if (config.isTLS()) {
            LOG.info("HTTPS enabled. Keystore file={}, keystore pass={} chars, Truststore file={}, Truststorepass={} chars",
                    config.getKeystoreFile(), config.getKeystorePass().length(),
                    config.getTruststoreFile(), "(not printed)");
            secure(config.getKeystoreFile(), config.getKeystorePass(), config.getTruststoreFile(), config.getTruststorePass());
        } else {
            LOG.info("HTTPS disabled. Scheme: {}", config.getUri().getScheme());
        }

        port(config.getUri().getPort());
        ipAddress(config.getUri().getHost());
        staticFileLocation("/public");

        WebConsole.config = config;
        JobRequestHandler.setMaxListJobSize(config.getMaxListJobSize());

        before(WebConsole::authenticate);

        after((req, res) -> {
            LOG.info("{} {} {} {} from {} {}",
                    res.raw().getStatus(),
                    req.requestMethod(), req.url(), req.raw().getQueryString(), req.ip(), req.userAgent());
        });

        exception(FileNotFoundException.class, (exception, request, response) -> {
            LOG.debug(exception.toString(), exception);
            handleException(404, exception.toString(), response);
        });

        exception(JobNotFoundException.class, (exception, request, response) -> {
            LOG.debug(exception.toString(), exception);
            handleException(404, exception.toString(), response);
        });

        exception(DownloadFileSizeExceeded.class, (exception, request, response) -> {
            LOG.warn(exception.toString(), exception);
            handleException(500, exception.toString(), response);
        });

        exception(Exception.class, (exception, request, response) -> {
            LOG.error(exception.toString(), exception);
            handleException(500, "Internal Server Error: " + exception.toString(), response);
        });

        // APIs to be in vanilla HTTP
        get("/ping", (req, res) -> "\"OK\"");
        get("/status", WebConsole::status);

        // TODO: XXX: validate application owner at ALL job-related APIs
        // /jobs POST -> list
        post(ListJobRequest.resourcePattern(), JobRequestHandler::listJob);
        // /job  PUT -> schedule, GET -> get-job, DELETE -> kill
        get(GetJobRequest.resourcePattern(), JobRequestHandler::getJob);
        post(ScheduleRequest.resourcePattern(), JobRequestHandler::schedule);
        delete(KillRequest.resourcePattern(), JobRequestHandler::kill);
        // Get a file
        get(GetFileRequest.resourcePattern(), JobRequestHandler::getFile);
        get(DownloadFileRequest.resourcePattern(), JobRequestHandler::downloadFile);
        // Get file list
        get(ListFilesRequest.resourcePattern(), JobRequestHandler::getDir);

        // /apps GET -> list-app
        get(ListAppRequest.resourcePattern(), AppRequestHandler::listApp);

        // /app  PUT -> load, GET -> get-app, DELETE -> unload-app
        put(LoadAppRequest.resourcePattern(), AppRequestHandler::loadApp);
        get(GetAppRequest.resourcePattern(), AppRequestHandler::getApp);
        delete(UnloadAppRequest.resourcePattern(), AppRequestHandler::unloadAppRequest);

        StatusCache.start(1);
        init();
    }

    public static void set(RetzScheduler sched, SchedulerDriver driver) {
        JobRequestHandler.setDriver(driver);
        JobRequestHandler.setScheduler(sched);
        scheduler = Optional.of(sched);
    }

    public static void handleException(int status, String mesg, Response response) {
        response.status(status);
        ErrorResponse errorResponse = new ErrorResponse(mesg);
        try {
            response.body(MAPPER.writeValueAsString(errorResponse));
        } catch (JsonProcessingException e) {
            LOG.error(e.toString(), e);
            response.body(e.toString());
        }
    }

    static void authenticate(Request req, Response res) throws IOException, URISyntaxException {
        res.header("Server", RetzScheduler.HTTP_SERVER_NAME);

        String resource;

        if (req.raw().getQueryString() != null) {
            resource = new StringBuilder().append(new URI(req.url()).getPath())
                    .append("?").append(req.raw().getQueryString()).toString();
        } else {
            resource = new URI(req.url()).getPath();
        }
        LOG.debug("{} {} from {} {}", req.requestMethod(), resource, req.ip(), req.userAgent());

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
        if (NO_AUTH_PAGES.contains(resource)) {
            return;
        }

        Optional<AuthHeader> authHeaderValue = getAuthInfo(req);
        if (!authHeaderValue.isPresent()) {
            halt(401, "Bad Authorization header: " + req.headers(AuthHeader.AUTHORIZATION));
        }

        Authenticator authenticator;
        // Not admin
        Optional<User> u = Database.getInstance().getUser(authHeaderValue.get().key());
        if (u.isPresent()) {
            if (!u.get().enabled()) {
                halt(403, "User disabled");
            }
        } else {
            halt(403, "No such user");
        }
        if (config.authenticationEnabled()) {
            authenticator = new HmacSHA256Authenticator(u.get().keyId(), u.get().secret());
        } else {
            authenticator = new NoopAuthenticator(u.get().keyId());
        }

        if (!authenticator.authenticate(verb, md5, date, resource,
                authHeaderValue.get().key(), authHeaderValue.get().signature())) {
            String string2sign = authenticator.string2sign(verb, md5, date, resource);
                LOG.debug("Auth failed. Calculated signature={}, Given signature={}, S2S={}",
                        authenticator.signature(verb, md5, date, resource),
                        authHeaderValue.get().signature(),
                        string2sign);

            halt(401, "Authentication failed. String to sign: " + string2sign);
        }
    }

    static String status(Request request, Response response) throws JsonProcessingException {
        return StatusCache.getStatusResponse();
    }

    static Optional<AuthHeader> getAuthInfo(Request req) {
        String givenSignature = req.headers(AuthHeader.AUTHORIZATION);
        LOG.debug("Signature from client: {}", givenSignature);

        return AuthHeader.parseHeaderValue(givenSignature);
    }

    static void validateOwner(Request req, Application app) {
        Optional<AuthHeader> authHeaderValue = getAuthInfo(req);
        if (!app.getOwner().equals(authHeaderValue.get().key())) {
            LOG.debug("Invalid request: requester and owner does not match");
            halt(400, "Invalid request: requester and owner does not match");
        }
    }

    public static void stop() {
        Spark.stop();
        StatusCache.stop();
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
        }
    }
}

