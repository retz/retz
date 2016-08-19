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

import io.github.retz.protocol.*;
import io.github.retz.scheduler.Applications;
import io.github.retz.scheduler.JobQueue;
import io.github.retz.scheduler.RetzScheduler;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import org.apache.mesos.SchedulerDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Request;
import spark.Response;
import spark.Spark;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static io.github.retz.scheduler.JobQueue.getAll;
import static spark.Spark.*;

public final class WebConsole {
    private static final Logger LOG = LoggerFactory.getLogger(WebConsole.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static Optional<RetzScheduler> scheduler = Optional.empty();
    private static Optional<SchedulerDriver> driver = Optional.empty();

    private Thread clientMonitorThread;
    private ClientMonitor clientMonitor;

    {
        MAPPER.registerModule(new Jdk8Module());
    }

    public WebConsole(int port) {
        port(port);
        staticFileLocation("/public");
        webSocket("/cui", ConsoleWebSocketHandler.class);
        webSocketIdleTimeoutMillis(Connection.IDLE_TIMEOUT_SEC * 1000);
        get("/ping", (req, res) -> "OK");
        get("/status", WebConsole::status);
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

    public static ListJobResponse list(int limit)
    {
        List<Job> queue = JobQueue.getAll();
        List<Job> running = new LinkedList<>();
        for (Map.Entry<String, Job> entry : JobQueue.getRunning().entrySet()) {
            running.add(entry.getValue());
        }
        List<Job> finished = new LinkedList<>();
        JobQueue.getAllFinished(finished, limit);

        return new ListJobResponse(queue, running, finished);
    }

    // Search job from JobQueue with matching id
    public static Optional<Job> getJob(int id) {
        return JobQueue.getJob(id);
    }

    public static boolean load(Application app) {
        return Applications.load(app);
    }

    public static List<Application> listApps() {
        List<Applications.Application> list = Applications.getAll();
        LOG.debug("currently {} applications", list.size());
        return list.stream()
                .map(app -> new Application(app.appName, app.persistentFiles, app.appFiles, app.diskMB))
                .collect(Collectors.toList());
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
