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
package io.github.retz.web.replica;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import io.github.retz.cli.TimestampHelper;
import io.github.retz.protocol.*;
import io.github.retz.protocol.data.Application;
import io.github.retz.protocol.data.Job;
import io.github.retz.scheduler.Applications;
import io.github.retz.scheduler.CuratorClient;
import io.github.retz.scheduler.JobQueue;
import io.github.retz.scheduler.RetzScheduler;
import io.github.retz.web.*;
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

    private Thread clientMonitorThread;
    private ClientMonitor clientMonitor;    
    
    static {
        MAPPER.registerModule(new Jdk8Module());
    }
    
    public WebConsole(int port) {
        port(port);        
        staticFileLocation("/public");
	
        // APIs to be in WebSocket: watch, run
        webSocket("/cui", ConsoleWebSocketHandler.class);
        webSocketIdleTimeoutMillis(Connection.IDLE_TIMEOUT_SEC * 1000);

        // APIs to be in vanilla HTTP
        get("/ping", (req, res) -> "OK");
        get("/status", WebConsole::status);

        // /jobs GET -> list
        get(ListJobRequest.resourcePattern(), (req, res) -> {
            Optional<URI> masterUri = CuratorClient.getMasterUri();
            if (masterUri.isPresent()) {
                res.redirect(masterUri.get().toString() + "/jobs", 301);
            } else {
                res.status(503);
            }
            return null;
        });
        // /job  PUT -> schedule, GET -> get-job, DELETE -> kill
        get(GetJobRequest.resourcePattern(), (req, res) -> {
            Optional<URI> masterUri = CuratorClient.getMasterUri();
            if (masterUri.isPresent()) {
                res.redirect(masterUri.get().toString() + req.params(":id"), 301); 
            } else {
                res.status(503);
            }
            return null;
        });

        put(ScheduleRequest.resourcePattern(), (req, res) -> {
            Optional<URI> masterUri = CuratorClient.getMasterUri();
            if (masterUri.isPresent()) {
                res.redirect(masterUri.get().toString() + "/job", 301);  
            } else {
                res.status(503);
            }
            return null;
        });

        delete(KillRequest.resourcePattern(), (req, res) -> {
            Optional<URI> masterUri = CuratorClient.getMasterUri();
            if (masterUri.isPresent()) {
                res.redirect(masterUri.get().toString() + "job/" + req.params(":id"), 301);  
            } else {
                res.status(503);
            }
            return null;
        });

        // /apps GET -> list-app
        get(ListAppRequest.resourcePattern(), (req, res) -> {
            Optional<URI> masterUri = CuratorClient.getMasterUri();
            if (masterUri.isPresent()) {
                res.redirect(masterUri.get().toString() + "/apps", 301);  
            } else {
                res.status(503);
            }
            return null;
        });

        // /app  PUT -> load, GET -> get-app, DELETE -> unload-app
        put(LoadAppRequest.resourcePattern(), (req, res) -> {
            Optional<URI> masterUri = CuratorClient.getMasterUri();
            if (masterUri.isPresent()) {
                res.redirect(masterUri.get().toString() + "/app/" + req.params(":name"), 301);  
            } else {
                res.status(503);
            }
            return null;
        });

        get(GetAppRequest.resourcePattern(), (req, res) -> {
            Optional<URI> masterUri = CuratorClient.getMasterUri();
            if (masterUri.isPresent()) {
                res.redirect(masterUri.get().toString() + "/app/" + req.params(":name"), 301);  
            } else {
                res.status(503);
            }
            return null;
        });

        delete(UnloadAppRequest.resourcePattern(), (req, res) -> {
            Optional<URI> masterUri = CuratorClient.getMasterUri();
            if (masterUri.isPresent()) {
                res.redirect(masterUri.get().toString() + "/app/" + req.params(":name"), 301);  
            } else {
                res.status(503);
            }            
            return null;
        });
        
        init();
    }

    public static String status(Request request, Response response) {
        try {
            return MAPPER.writeValueAsString(new ErrorResponse("no scheduler set now"));
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
