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
import io.github.retz.auth.Authenticator;
import io.github.retz.cli.TimestampHelper;
import io.github.retz.protocol.*;
import io.github.retz.protocol.data.Application;
import io.github.retz.protocol.data.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.DatatypeConverter;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.KeyManagementException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Objects;
import java.util.Optional;
import java.util.ResourceBundle;

import static java.nio.charset.StandardCharsets.UTF_8;

public class Client implements AutoCloseable {
    public static final String VERSION_STRING;
    static final Logger LOG = LoggerFactory.getLogger(Client.class);

    static {
        ResourceBundle labels = ResourceBundle.getBundle("retz-client");
        VERSION_STRING = labels.getString("version");
    }

    private final ObjectMapper mapper;
    private final String scheme;
    private final String host;
    private final int port;
    private final Optional<io.github.retz.auth.Authenticator> authenticator;
    private MessageDigest DIGEST = null; //TODO this could be final, though...

    protected Client(URI uri, Optional<Authenticator> authenticator) {
        this.scheme = uri.getScheme();
        this.host = uri.getHost();
        this.port = uri.getPort();
        this.mapper = new ObjectMapper();
        this.mapper.registerModule(new Jdk8Module());
        this.authenticator = authenticator;

        try {
            DIGEST = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new AssertionError("No such algorithm callled MD5.");
        }
    }

    protected Client(URI uri) {
        this(uri, Optional.empty());
    }

    protected Client(URI uri, Optional<Authenticator> authenticator, boolean checkCert) {
        this(uri, authenticator);
        if (uri.getScheme().equals("https") && !checkCert) {
            try {
                WrongTrustManager.disableTLS();
            } catch (NoSuchAlgorithmException e) {
                throw new AssertionError(e.toString());
            } catch (KeyManagementException e) {
                throw new AssertionError(e.toString());
            }
        }
    }

    public static ClientBuilder newBuilder(URI uri) {
        return new ClientBuilder(uri);
    }

    @Override
    public void close() {
    }


    public boolean ping() throws IOException {
        URL url;
        try {
            url = new URL(scheme + "://" + host + ":" + port + "/ping");
            LOG.debug("Pinging {}", url);
        } catch (MalformedURLException e) {
            LOG.error(e.toString());
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
            return "OK".equals(msg);
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
        return rpc(new ScheduleRequest(job));
    }

    public Response getJob(int id) throws IOException {
        return rpc(new GetJobRequest(id));
    }

    public Response getFile(int id, String file, int offset, int length) throws IOException {
        return rpc(new GetFileRequest(id, file, offset, length));
    }

    public Response listFiles(int id, String path) throws IOException {
        return rpc(new ListFilesRequest(id, path));
    }

    public Job run(Job job) throws IOException {
        Response res = rpc(new ScheduleRequest(job));
        if (!(res instanceof ScheduleResponse)) {
            LOG.error(res.status());
            return null;
        }
        ScheduleResponse scheduleResponse = (ScheduleResponse) res;
        LOG.info("Job scheduled: id={}", scheduleResponse.job().id());

        return waitPoll(scheduleResponse.job());

    }

    private Job waitPoll(Job job) throws IOException {
        do {
            Response res = rpc(new GetJobRequest(job.id()));
            if (res instanceof GetJobResponse) {
                GetJobResponse getJobResponse = (GetJobResponse) res;
                if (getJobResponse.job().isPresent()) {
                    if (getJobResponse.job().get().state() == Job.JobState.FINISHED
                            || getJobResponse.job().get().state() == Job.JobState.KILLED) {

                        return getJobResponse.job().get();
                    } else {
                        try {
                            Thread.sleep(1024);
                        } catch (InterruptedException e) {
                        }
                    }
                } else {
                    LOG.error("Job id={} does not exist.", job.id());
                    return null;
                }
            } else {
                LOG.error(res.status());
                return null;
            }
        } while (true);
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

    public Response listApp() throws IOException {
        return rpc(new ListAppRequest());
    }

    @Deprecated
    public Response unload(String appName) throws IOException {
        return rpc(new UnloadAppRequest(appName));
    }

    private <ReqType extends Request> Response rpc(ReqType req) throws IOException {
        URL url;
        try {
            url = new URL(scheme + "://" + host + ":" + port + req.resource());
            LOG.debug("Connecting {}", url);
        } catch (MalformedURLException e) {
            return new ErrorResponse(e.toString());
        }
        HttpURLConnection conn = null;
        try {
            conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod(req.method());
            conn.setDoOutput(req.hasPayload());

            String md5 = "";
            String payload = "";
            if (req.hasPayload()) {
                payload = mapper.writeValueAsString(req);
                LOG.debug("Sending {} request with payload '{}'", req.method(), payload);
                md5 = DatatypeConverter.printHexBinary(DIGEST.digest(payload.getBytes(UTF_8)));
                conn.setRequestProperty("Content-MD5", md5);
                conn.setRequestProperty("Content-Length", Integer.toString(payload.length()));
            }

            String date = TimestampHelper.now();
            conn.setRequestProperty("Date", date);

            if (authenticator.isPresent()) {
                String signature = authenticator.get().buildHeaderValue(req.method(), md5, date, req.resource());
                conn.setRequestProperty(Authenticator.AUTHORIZATION, signature);
            }

            if (req.hasPayload()) {
                conn.getOutputStream().write(payload.getBytes(UTF_8));
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
}
