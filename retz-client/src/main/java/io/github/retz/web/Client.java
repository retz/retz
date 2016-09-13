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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import io.github.retz.protocol.*;
import io.github.retz.protocol.data.Application;
import io.github.retz.protocol.data.Job;
import io.github.retz.protocol.data.MesosContainer;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.client.ClientUpgradeRequest;
import org.eclipse.jetty.websocket.client.WebSocketClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.function.Predicate;


// WebSocket client to Retz service
public class Client implements AutoCloseable {
    static final Logger LOG = LoggerFactory.getLogger(Client.class);
    private final ObjectMapper mapper;
    private final String host;
    private final int port;
    private WebSocketClient wsclient = null;
    private MySocket socket = null;

    public Client(String host, int port) {
        this.host = host;
        this.port = port;
        this.mapper = new ObjectMapper();
        this.mapper.registerModule(new Jdk8Module());
    }

    public static void fetchJobResult(Job job, String resultDir) {
        if (resultDir == null) {
            return;
        }
        if (job.url() == null) {
            LOG.error("Can't fetch outputs: no url found");
            return;
        }
        if (resultDir.equals("-")) {
            LOG.info("==== Printing stdout of remote executor ====");
            catHTTPFile(job.url(), "stdout");
            LOG.info("==== Printing stderr of remote executor ====");
            catHTTPFile(job.url(), "stderr");

            if (statHTTPFile(job.url(), "stdout-" + job.id())) {
                LOG.info("==== Printing stdout-{} of remote executor ====", job.id());
                catHTTPFile(job.url(), "stdout-" + job.id());
            }
            if (statHTTPFile(job.url(), "stderr-" + job.id())) {
                LOG.info("==== Printing stderr-{} of remote executor ====", job.id());
                catHTTPFile(job.url(), "stderr-" + job.id());
            }
        } else {
            fetchHTTPFile(job.url(), "stdout", resultDir);
            Client.fetchHTTPFile(job.url(), "stderr", resultDir);
            if (statHTTPFile(job.url(), "stdout-" + job.id())) {
                Client.fetchHTTPFile(job.url(), "stdout-" + job.id(), resultDir);
            }
            if (statHTTPFile(job.url(), "stderr-" + job.id())) {
                Client.fetchHTTPFile(job.url(), "stderr-" + job.id(), resultDir);
            }
        }
    }

    public static boolean statHTTPFile(String url, String name) {
        String addr = url.replace("files/browse", "files/download") + "%2F" + name;

        HttpURLConnection conn = null;
        try {
            conn = (HttpURLConnection) new URL(addr).openConnection();
            conn.setRequestMethod("HEAD");
            conn.setDoOutput(false);
            LOG.debug(conn.getResponseMessage());
            return conn.getResponseCode() == 200 ||
                    conn.getResponseCode() == 204;
        } catch (IOException e) {
            LOG.debug("Failed to fetch {}: {}", addr, e.toString());
            return false;
        } finally {
            if (conn != null) {
                conn.disconnect();
            }
        }
    }

    public static void catHTTPFile(String url, String name) {
        catHTTPFile(url, name, System.out);
    }

    public static void catHTTPFile(String url, String name, OutputStream out) {
        String addr = url.replace("files/browse", "files/download") + "%2F" + name;

        HttpURLConnection conn = null;
        try {
            conn = (HttpURLConnection) new URL(addr).openConnection();
            conn.setRequestMethod("GET");
            conn.setDoOutput(true);
        } catch (IOException e) {
            LOG.error("Failed to fetch {}: {}", addr, e.toString());
            return;
        }
        //String s = conn.getHeaderField("Content-length");
        //LOG.info("Content-length: {}", s);
        try (InputStream input = conn.getInputStream()) {
            byte[] buffer = new byte[65536];
            int bytesRead = 0;
            while ((bytesRead = input.read(buffer)) != -1) {
                out.write(buffer, 0, bytesRead);
            }
        } catch (IOException e) {
            // Somehow this happens even HTTP was correct
            LOG.debug("Cannot fetch file {}: {}", addr, e.toString());
            // Just retry until your stack get stuck; thanks to SO:33340848
            // and to that crappy HttpURLConnection
            catHTTPFile(url, name, out);
        } finally {
            conn.disconnect();
        }
    }

    public static void fetchHTTPFile(String url, String name, String dir) {
        boolean _res = new File(dir).mkdir();
        String addr = url.replace("files/browse", "files/download") + "%2F" + name;
        String localfile = FilenameUtils.concat(dir, name);
        LOG.info("Downloading {} as {}", addr, localfile);
        try {
            FileUtils.copyURLToFile(new URL(addr), new File(localfile));
        } catch (MalformedURLException e) {
            LOG.error(e.toString());
        } catch (IOException e) {
            LOG.error(e.toString());
        }
    }

    private boolean connect() throws IOException, ExecutionException, URISyntaxException, Exception {
        URI uri = new URI(String.format("ws://%s:%d/cui", host, port));
        this.socket = new MySocket();
        this.wsclient = new WebSocketClient();
        wsclient.start();

        Future<Session> f = wsclient.connect(socket, uri, new ClientUpgradeRequest());
        while (true) {
            try {
                f.get();
                break;
            } catch (InterruptedException e) {
            }
        }
        return !f.isCancelled();
    }

    @Override
    public void close() {
        disconnect();
    }

    public void disconnect() {
        if (wsclient != null) {
            try {
                wsclient.stop();
            } catch (Exception e) {
            }
        }
    }

    public boolean ping() throws IOException {
        URL url;
        try {
            // TODO: make this switchable on http/https
            url = new URL("http://" + host + ":" + port + "/ping");
            LOG.debug("Pinging {}", url);
        } catch (MalformedURLException e) {
            return false;
        }
        HttpURLConnection conn = null;
        try {
            conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");
            conn.setDoOutput(false);
            byte[] buffer = {'n', 'g'};
            int s = conn.getInputStream().read(buffer, 0, 2);
            if (s < 0) {
                return false;
            }
            String msg = new String(buffer, StandardCharsets.UTF_8);
            LOG.info(msg);
            return "OK" .equals(msg);
        } catch (IOException e) {
            LOG.debug(e.toString());
            return false;
        } finally {
            if (conn != null) {
                conn.disconnect();
            }
        }
    }

    public Response list(int limit) throws IOException {
        return rpc(new ListJobRequest(limit));
    }

    public Response schedule(Job job) throws IOException {
        return rpc(new ScheduleRequest(job, false));
    }

    public Response getJob(int id) throws IOException {
        return rpc(new GetJobRequest(id));
    }

    public Job run(Job job) throws IOException {
        Response res = rpc(new ScheduleRequest(job, true));
        // FIXME: There's a slight race condition between a job finish and watch registration
        // The latter is expected to be before the former, otherwise this function waits forever
        if (res instanceof ScheduleResponse) {
            ScheduleResponse scheduleResponse = (ScheduleResponse) res;
            LOG.info("Job scheduled: id={}", scheduleResponse.job().id());
            Job result = waitForResponse(watchResponse -> {
                if (watchResponse.job() == null) {
                    return null;
                } else if (scheduleResponse.job().id() == watchResponse.job().id()) {
                    LOG.info("{}: id={}", watchResponse.event(), watchResponse.job().id());
                    if (watchResponse.job().state() == Job.JobState.FINISHED) {
                        return watchResponse.job();
                    } else if (watchResponse.job().state() == Job.JobState.KILLED) {
                        return watchResponse.job();
                    }
                }
                LOG.debug("keep waiting");
                return null; // keep waiting
            });
            return result;
        } else {
            LOG.error(res.status());
            return null;
        }
    }

    public Response kill(int id) throws IOException {
        return rpc(new KillRequest(id));
    }

    public Response getApp(String appid) throws IOException {
        return rpc(new GetAppRequest(appid));
    }

    public Response load(Application application) throws IOException {
        return rpc(new LoadAppRequest(Objects.requireNonNull(application)));
    }

    public Response load(String appid, List<String> persistentFiles, List<String> largeFiles,
                         List<String> files, Optional<Integer> diskMB) throws IOException {
        return rpc(new LoadAppRequest(new Application(appid, persistentFiles, largeFiles, files, diskMB, new MesosContainer())));
    }

    public Response load(String appid, List<String> persistentFiles, List<String> largeFiles, List<String> files) throws IOException {
        return load(appid, persistentFiles, largeFiles, files, Optional.empty());
    }

    public Response listApp() throws IOException {
        return rpc(new ListAppRequest());
    }

    public Response unload(String appName) throws IOException {
        return rpc(new UnloadAppRequest(appName));
    }

    private <ReqType extends Request> Response rpc(ReqType req) throws IOException {
        URL url;
        try {
            // TODO: make this switchable on http/https
            url = new URL("http://" + host + ":" + port + req.resource());
            LOG.debug("Connecting {}", url);
        } catch (MalformedURLException e) {
            return new ErrorResponse(e.toString());
        }
        HttpURLConnection conn = null;
        try {
            conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod(req.method());
            conn.setDoOutput(req.hasPayload());
            if (req.hasPayload()) {
                mapper.writeValue(conn.getOutputStream(), req);
            }
            return mapper.readValue(conn.getInputStream(), Response.class);
        } catch (IOException e) {
            LOG.debug(e.toString());
            return new ErrorResponse(e.toString());
        } finally {
            if (conn != null) {
                conn.disconnect();
            }
        }
    }

    // Wait for good response until the callback gives non-null good value.
    // To keep watching, callback must keep returning null.
    public <RetType> RetType waitForResponse(Function<WatchResponse, RetType> callback) throws IOException {
        try {
            if (!connect()) {
                LOG.error("failed to connect to host");
                return null;
            } else {
                LOG.info("Connected to the host. Start waiting for response..");
            }
        } catch (Exception e) {
            LOG.error(e.toString());
            return null;
        }
        RetType ret = null;
        while (ret == null) {
            String json;
            try {
                json = socket.awaitResponse();
                LOG.debug("WaitForResponse started: {}", json);
            } catch (InterruptedException e) {
                continue;
            }
            Response res = mapper.readValue(json, Response.class);
            if (res instanceof WatchResponse) {
                WatchResponse wres = (WatchResponse) res;
                ret = callback.apply(wres);
            } else if (res instanceof ErrorResponse) {
                ErrorResponse eres = (ErrorResponse) res;
                LOG.error(eres.status());
                return null;
            }
        }
        return ret;
    }

    // This method does block while callback returns.
    // The callback must return true to keep watching.
    public void startWatch(Predicate<WatchResponse> callback) throws IOException {
        waitForResponse(watchResponse -> {
            if (callback.test(watchResponse)) {
                return null;
            } else {
                return 1;
            }
        });

    }
}
