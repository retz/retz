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
import io.github.retz.cli.TimestampHelper;
import io.github.retz.protocol.*;
import io.github.retz.scheduler.Applications;
import io.github.retz.scheduler.JobQueue;
import org.eclipse.jetty.util.ConcurrentHashSet;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.WebSocketException;
import org.eclipse.jetty.websocket.api.annotations.*;
import org.eclipse.jetty.websocket.api.extensions.Frame;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@WebSocket
public class ConsoleWebSocketHandler {
    private static final Logger LOG = LoggerFactory.getLogger(ConsoleWebSocketHandler.class);

    private static final Set<Session> SESSIONS = new ConcurrentHashSet<>();
    private static final Set<Session> WATCHERS = new ConcurrentHashSet<>();
    private static final Map<Integer, Session> JOB_WATCHERS = new ConcurrentHashMap<>();
    private final ObjectMapper MAPPER = new ObjectMapper();

    {
        MAPPER.registerModule(new Jdk8Module());
    }

    public static void broadcast(String event, Job job, String status) {
        // TODO: make use of static members
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new Jdk8Module());

        WatchResponse res = new WatchResponse(event, job);
        res.status(status);
        try {
            String msg = mapper.writeValueAsString(res);
            broadcast(msg);
        } catch (JsonProcessingException e) {
            LOG.error("never comes here");
        }
    }

    private static void broadcast(String msg) {
        LOG.info("Broadcasting {}", msg);
        for (Session s : WATCHERS) {
            try {
                s.getRemote().sendString(msg);
            } catch (IOException e) {
                LOG.warn("cannot send to {}: {}", s.getRemoteAddress().getHostName(), e.toString());
            }
        }
    }

    public static void sendPingAll() {
        for (Session s : WATCHERS) {
            try {
                ByteBuffer ping = ByteBuffer.wrap("ping".getBytes(StandardCharsets.UTF_8));
                s.getRemote().sendPing(ping);
            } catch (IOException e) {
                LOG.warn("cannot send to {}: {}", s.getRemoteAddress().getHostName(), e.toString());
                s.close();
                WATCHERS.remove(s);
            }
        }
        for (Map.Entry<Integer, Session> watcher : JOB_WATCHERS.entrySet()) {
            try {
                ByteBuffer ping = ByteBuffer.wrap("ping".getBytes(StandardCharsets.UTF_8));
                watcher.getValue().getRemote().sendPing(ping);
            } catch (IOException e) {
                LOG.warn("cannot send to {}: {}", watcher.getValue().getRemoteAddress().getHostName(), e.toString());
                watcher.getValue().close();
                JOB_WATCHERS.remove(watcher.getKey());
            }
        }

    }

    public static void notify(String update, Job job) {
        if (JOB_WATCHERS.containsKey(job.id())) {
            WatchResponse res = new WatchResponse(update, job);
            Session s = JOB_WATCHERS.get(job.id());
            // TODO: don't instantiate ObjectMapper every time
            ObjectMapper mapper = new ObjectMapper();
            mapper.registerModule(new Jdk8Module());
            try {
                String json = mapper.writeValueAsString(res);
                s.getRemote().sendString(json);

                if (update.equals("finished") || update.equals("killed")) {
                    JOB_WATCHERS.remove(job.id());
                }
            } catch (JsonProcessingException e) {
                LOG.error("Cannot encode Job {}: {}", job, e.toString());
            } catch (IOException e) {
                LOG.warn("Cannot send {} to {}", update, s.getRemoteAddress());
            } catch (WebSocketException e) {
                LOG.error("Client were disconnected: {}", e.toString());
            }
        }
    }

    @OnWebSocketConnect
    public void onConnect(Session user) throws Exception {
        LOG.info("Connect from {}", user.getLocalAddress().getHostString());
        SESSIONS.add(user);
    }

    @OnWebSocketClose
    public void onClose(Session user, int statusCode, String reason) {
        SESSIONS.remove(user);
        WATCHERS.remove(user);
        LOG.info("{} disconnected: {}, {}",
                user.getLocalAddress().getHostString(), statusCode, reason);
    }

    @OnWebSocketFrame
    public void onSocketFrame(Session user, Frame frame) throws IOException {
        if (frame.getType() == Frame.Type.PING) {
            ByteBuffer bb = ByteBuffer.wrap("pong".getBytes(StandardCharsets.UTF_8));
            user.getRemote().sendPong(bb);
        } else if (frame.getType() == Frame.Type.PONG) {
            // OK
        }
    }

    @OnWebSocketMessage
    public void onMessage(Session user, String message) throws IOException, InterruptedException {
        LOG.info("Command: {}", message);
        Request req = null;
        try {
            req = MAPPER.readValue(message, Request.class);
        } catch (IOException e) {
            respond(user, new ErrorResponse("Invalid command: " + e.toString()));
            return;
        } catch (NullPointerException e) {
            LOG.error("NullPointerException: {}", user);
            e.printStackTrace();
            respond(user, new ErrorResponse("NullPointerException"));
            return;
        }

        if (req instanceof ListJobRequest) {
            ListJobRequest listJobRequest = (ListJobRequest) req;
            ListJobResponse res = WebConsole.list(listJobRequest.limit());
            res.ok();
            LOG.info("Command: list -> {}", res.queue());
            respond(user, res);

        } else if (req instanceof KillRequest) {
            KillResponse res = new KillResponse();
            res.status("kill not supported yet.");
            respond(user, res);

        } else if (req instanceof ScheduleRequest) {
            ScheduleRequest scheduleRequest = (ScheduleRequest) req;
            if (Applications.get(scheduleRequest.job().appid()).isPresent()) {
                Job job = scheduleRequest.job();
                job.schedule(JobQueue.issueJobId(), TimestampHelper.now());

                if (scheduleRequest.doWatch()) {
                    JOB_WATCHERS.put(job.id(), user);
                }

                JobQueue.push(job);

                ScheduleResponse scheduleResponse = new ScheduleResponse(job);
                scheduleResponse.ok();
                LOG.info("Job '{}' at {} has been scheduled at {}.", job.cmd(), job.appid(), job.scheduled());
                respond(user, scheduleResponse);
                broadcast("scheduled", job, "ok");

            } else {
                ErrorResponse res = new ErrorResponse("No such application: " + scheduleRequest.job().appid());
                respond(user, res);
            }

        } else if (req instanceof GetJobRequest) {
            GetJobRequest getJobRequest = (GetJobRequest)req;
            Optional<Job> job = WebConsole.getJob(getJobRequest.id());
            respond(user, new GetJobResponse(job));

        } else if (req instanceof WatchRequest) {
            WatchResponse res = new WatchResponse("start", null);
            res.status("Start watching");
            WATCHERS.add(user);
            respond(user, res);

        } else if (req instanceof LoadAppRequest) {
            LoadAppRequest loadAppRequest = (LoadAppRequest) req;
            LoadAppResponse res = new LoadAppResponse();
            boolean result = WebConsole.load(loadAppRequest.application());
            if (result) {
                res.ok();
                respond(user, res);
            } else {
                respond(user, new ErrorResponse("cannot load application"));
            }

        } else if (req instanceof ListAppRequest) {
            ListAppResponse res = new ListAppResponse(WebConsole.listApps());
            res.ok();
            respond(user, res);

        } else if (req instanceof UnloadAppRequest) {
            UnloadAppRequest unloadAppRequest = (UnloadAppRequest) req;
            WebConsole.unload(unloadAppRequest.appid());
            UnloadAppResponse res = new UnloadAppResponse();
            res.ok();
            respond(user, res);

        } else

        {
            // XXXXX: Cannot reach here
            LOG.error("unknown request: {}", req.getClass().getName());
            throw new AssertionError();
        }
        return;
    }

    private <ResType extends Response> void respond(Session user, ResType res) throws IOException {
        String json = MAPPER.writeValueAsString(res);
        if (json.length() <= Connection.MAX_PAYLOAD_SIZE) {
            user.getRemote().sendString(json);
        } else {
            String msg = String.format("%s: Payload JSON is larger than max size supported in client (%d > %d).",
                    res.getClass().getName(), json.length(), Connection.MAX_PAYLOAD_SIZE);
            LOG.warn(msg);
            JobQueue.compact();
            ErrorResponse eres = new ErrorResponse(msg);
            respond(user, eres);
        }
    }

    public static void setStatus(StatusResponse statusResponse) {
        statusResponse.setStatus2(WATCHERS.size(), SESSIONS.size());
    }
}
