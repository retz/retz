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
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Predicate;

// WebSocket client to Retz service
public class Client implements AutoCloseable {
    static final Logger LOG = LoggerFactory.getLogger(Client.class);

    private final URI uri;
    private final WebSocketClient wsclient;
    private final MySocket socket;
    private final ObjectMapper mapper;

    public Client(String host, int port) throws URISyntaxException {
        this(String.format("ws://%s:%d/cui", host, port));
    }

    public Client(String uri) throws URISyntaxException {
        this.uri = new URI(uri);
        this.wsclient = new WebSocketClient();
        this.socket = new MySocket();
        this.mapper = new ObjectMapper();
        this.mapper.registerModule(new Jdk8Module());
        try {
            wsclient.start();
        } catch (Exception e) {
            LOG.error(e.getMessage());
        }
    }

    public boolean connect() throws IOException, ExecutionException {

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
        try {
            wsclient.stop();
        } catch (Exception e) {
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
        ScheduleRequest request = new ScheduleRequest(job, true);
        if (!socket.sendRequest(mapper.writeValueAsString(request))) {
            throw new IOException("Request still sending");
        }

        Job ret = job;
        while (true) {
            String json;
            try {
                json = socket.awaitResponse();
            } catch (InterruptedException e) {
                continue;
            }
            Response response = mapper.readValue(json, Response.class);

            if (response instanceof ScheduleResponse) {
                ScheduleResponse sres = (ScheduleResponse) response;
                ret = sres.job();
                LOG.info("Job (id={}) was scheduled", ret.id());

            } else if (response instanceof WatchResponse) {
                WatchResponse wres = (WatchResponse) response;

                if (wres.job().id() != ret.id()) {
                    LOG.warn("Too fast job (id={}) finish?: wres.job.id={}, event={}", ret.id(), wres.job().id(), wres.event());
                } else if (wres.event().equals("started")) {
                    LOG.info("Job (id={}) has started", wres.job().id());
                } else if (wres.event().equals("finished")) {
                    LOG.info("Job (id={}) has finished: return value is {}", wres.job().id(), wres.job().result());
                    return wres.job();
                } else if (wres.event().equals("killed")) {
                    LOG.warn("Job (id={}) was killed by Retz or Mesos, reason: {}", wres.job().id(), wres.job().reason());
                    return wres.job();
                } else {
                    throw new AssertionError(wres.event());
                }

            } else if (response instanceof ErrorResponse) {
                LOG.error(((ErrorResponse) response).status());
                return null;
            } else {
                throw new AssertionError("Unknown response type of instance");
            }
        }

    }

    public Response kill(int id) throws IOException {
        return rpc(new KillRequest(id));
    }

    // This method does block while callback returns
    public void startWatch(Predicate<WatchResponse> callback) throws IOException {
        socket.sendRequest(mapper.writeValueAsString(new WatchRequest()));
        boolean doCont = true;
        while (doCont) {
            String json;
            try {
                json = socket.awaitResponse();
            } catch (InterruptedException e) {
                continue;
            }
            Response res = mapper.readValue(json, Response.class);
            if (res instanceof WatchResponse) {
                WatchResponse wres = (WatchResponse) res;
                doCont = callback.test(wres);
            } else if (res instanceof ErrorResponse) {
                ErrorResponse eres = (ErrorResponse) res;
                LOG.error(eres.status());
                return;
            }
        }
    }

    public Response load(String appid, List<String> persistentFiles, List<String> files, Optional<Integer> diskMB) throws IOException {
        return rpc(new LoadAppRequest(new Application(appid, persistentFiles, files, diskMB)));
    }

    public Response load(String appid, List<String> persistenFiles, List<String> files) throws IOException {
        return rpc(new LoadAppRequest(new Application(appid, persistenFiles, files, Optional.empty())));
    }

    public Response listApp() throws IOException {
        return rpc(new ListAppRequest());
    }

    public Response unload(String appName) throws IOException {
        return rpc(new UnloadAppRequest(appName));
    }

    private <ReqType> Response rpc(ReqType request) throws IOException {
        if (!socket.sendRequest(mapper.writeValueAsString(request))) {
            throw new IOException("Request still sending");
        }
        while (true) {
            try {
                String json = socket.awaitResponse();
                Response res = mapper.readValue(json, Response.class);
                return res;
            } catch (InterruptedException e) {
            }
        }
    }

    public static void fetchJobResult(Job job, String resultDir) {
        if (resultDir == null) {
            return;
        }
        if (job.url() == null) {
            LOG.error("Can't fetch outputs: no url found");
        }
        if (resultDir.equals("-")) {
            LOG.info("==== Printing stdout of remote executor ====");
            catHTTPFile(job.url(), "stdout");
            LOG.info("==== Printing stderr of remote executor ====");
            catHTTPFile(job.url(), "stderr");
            LOG.info("==== Printing stdout-{} of remote executor ====", job.id());
            catHTTPFile(job.url(), "stdout-" + job.id());
            LOG.info("==== Printing stderr-{} of remote executor ====", job.id());
            catHTTPFile(job.url(), "stderr-" + job.id());
        } else {
            fetchHTTPFile(job.url(), "stdout", resultDir);
            Client.fetchHTTPFile(job.url(), "stderr", resultDir);
            Client.fetchHTTPFile(job.url(), "stdout-" + job.id(), resultDir);
            Client.fetchHTTPFile(job.url(), "stderr-" + job.id(), resultDir);
        }
    }

    public static void catHTTPFile(String url, String name) {
        catHTTPFile(url, name, System.out);
    }

    public static void catHTTPFile(String url, String name, OutputStream out) {
        String addr = url.replace("files/browse", "files/download") + "/" + name;

        HttpURLConnection conn = null;
        try {
            conn = (HttpURLConnection) new URL(addr).openConnection();
            conn.setRequestMethod("GET");
        } catch (IOException e) {
            LOG.error("Failed to fetch {}: {}", addr, e.getMessage());
            return;
        }
        conn.setDoOutput(true);

        try (InputStream input = conn.getInputStream()) {
            byte[] buffer = new byte[65536];
            int bytesRead = 0;
            while ((bytesRead = input.read(buffer)) != -1) {
                out.write(buffer, 0, bytesRead);
            }
        } catch (IOException e) {
            LOG.error("Cannot fetch file {}: {}", url, e.getMessage());
        }
        conn.disconnect();
    }

    public static void fetchHTTPFile(String url, String name, String dir) {
        boolean _res = new File(dir).mkdir();
        String addr = url.replace("files/browse", "files/download") + "/" + name;
        String localfile = FilenameUtils.concat(dir, name);
        LOG.info("Downloading {} as {}", addr, localfile);
        try {
            FileUtils.copyURLToFile(new URL(addr), new File(localfile));
        } catch (MalformedURLException e) {
            LOG.error(e.getMessage());
        } catch (IOException e) {
            LOG.error(e.getMessage());
        }
    }
}
