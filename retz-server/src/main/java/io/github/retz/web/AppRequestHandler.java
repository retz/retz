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
import io.github.retz.auth.AuthHeader;
import io.github.retz.protocol.*;
import io.github.retz.protocol.data.Application;
import io.github.retz.protocol.data.DockerContainer;
import io.github.retz.scheduler.Applications;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Request;
import spark.Response;

import java.io.IOException;
import java.util.Optional;

import static io.github.retz.web.WebConsole.getAuthInfo;
import static io.github.retz.web.WebConsole.validateOwner;

public class AppRequestHandler {
    private static final Logger LOG = LoggerFactory.getLogger(AppRequestHandler.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    static {
        MAPPER.registerModule(new Jdk8Module());
    }

    static String listApp(Request req, Response res) throws JsonProcessingException {
        Optional<AuthHeader> authHeaderValue = getAuthInfo(req);
        LOG.info("Listing all apps owned by {}", authHeaderValue.get().key());
        ListAppResponse response = new ListAppResponse(Applications.getAll(authHeaderValue.get().key()));
        response.ok();
        return MAPPER.writeValueAsString(response);
    }

    static String loadApp(Request req, Response res) throws JsonProcessingException, IOException {
        LOG.debug(LoadAppRequest.resourcePattern());
        Optional<AuthHeader> authHeaderValue = getAuthInfo(req);
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

    }

    static String getApp(Request req, Response res) throws JsonProcessingException {
        LOG.debug(LoadAppRequest.resourcePattern());
        Optional<AuthHeader> authHeaderValue = getAuthInfo(req);

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
            String r = MAPPER.writeValueAsString(getAppResponse);
            LOG.info(r);
            return r;

        } else {
            ErrorResponse response = new ErrorResponse("No such application: " + appname);
            res.status(404);
            return MAPPER.writeValueAsString(response);
        }
    }

    static String unloadAppRequest(Request req, Response res) throws JsonProcessingException {
        String appname = req.params(":name");
        LOG.warn("deleting app {} (This API is deprecated)", appname);
        // TODO: deletion may be done only if it has no job
        Applications.unload(appname);
        UnloadAppResponse response = new UnloadAppResponse();
        response.ok();
        return MAPPER.writeValueAsString(response);
    }
}
