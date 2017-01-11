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

import feign.FeignException;
import io.github.retz.auth.AuthHeader;
import io.github.retz.auth.Authenticator;
import io.github.retz.cli.TimestampHelper;
import io.github.retz.misc.Pair;
import io.github.retz.protocol.DownloadFileRequest;
import io.github.retz.protocol.GetJobResponse;
import io.github.retz.protocol.Response;
import io.github.retz.protocol.ScheduleResponse;
import io.github.retz.protocol.data.Application;
import io.github.retz.protocol.data.Job;
import io.github.retz.web.feign.Retz;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import java.io.BufferedInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.Objects;
import java.util.Optional;
import java.util.ResourceBundle;

import static java.nio.charset.StandardCharsets.UTF_8;

public class Client implements AutoCloseable {

    public static final String VERSION_STRING;
    public static final int MAX_BIN_SIZE = (int) DownloadFileRequest.MAX_FILE_SIZE;

    static final Logger LOG = LoggerFactory.getLogger(Client.class);

    static {
        ResourceBundle labels = ResourceBundle.getBundle("retz-client");
        VERSION_STRING = labels.getString("version");
    }

    private final Retz retz;
    private boolean verboseLog = false;
    private URI uri;
    private Authenticator authenticator;

    protected Client(URI uri, Authenticator authenticator) {
        this(uri, authenticator, true);
    }

    protected Client(URI uri, Authenticator authenticator, boolean checkCert) {
        this.uri = Objects.requireNonNull(uri);
        this.authenticator = Objects.requireNonNull(authenticator);
        SSLSocketFactory socketFactory;
        HostnameVerifier hostnameVerifier;
        if (uri.getScheme().equals("https") && !checkCert) {
            LOG.warn("DANGER ZONE: TLS certificate check is disabled. Set 'retz.tls.insecure = false' at config file to supress this message.");
            try {
                SSLContext sc = SSLContext.getInstance("SSL");
                sc.init(null, new TrustManager[]{new WrongTrustManager()}, new java.security.SecureRandom());
                socketFactory = sc.getSocketFactory();
                hostnameVerifier = new NoOpHostnameVerifier();
            } catch (NoSuchAlgorithmException e) {
                throw new AssertionError(e.toString());
            } catch (KeyManagementException e) {
                throw new AssertionError(e.toString());
            }
        } else {
            socketFactory = null;
            hostnameVerifier = null;
        }
        this.retz = Retz.connect(uri, authenticator, socketFactory, hostnameVerifier);
        System.setProperty("http.agent", Client.VERSION_STRING);
    }

    public static ClientBuilder newBuilder(URI uri) {
        return new ClientBuilder(uri);
    }

    public void setVerboseLog(boolean b) {
        verboseLog = b;
    }

    @Override
    public void close() {
    }

    public boolean ping() throws IOException {
        try {
            return "OK".equals(retz.ping());
        } catch (FeignException e) {
            LOG.debug(e.toString());
            return false;
        }
    }

    public Response status() throws IOException {
        return Retz.tryOrErrorResponse(() -> retz.status());
    }

    public Response list(Job.JobState state, Optional<String> tag) throws IOException {
        return Retz.tryOrErrorResponse(() -> retz.list(state, tag));
    }

    public Response schedule(Job job) throws IOException {
        if (job.priority() < -20 || 19 < job.priority()) {
            throw new IllegalArgumentException("Priority must be [-19, 20]");
        }
        return Retz.tryOrErrorResponse(() -> retz.schedule(Objects.requireNonNull(job)));
    }

    public Response getJob(int id) throws IOException {
        return Retz.tryOrErrorResponse(() -> retz.getJob(id));
    }

    public Response getFile(int id, String file, long offset, long length) throws IOException {
        return Retz.tryOrErrorResponse(
                () -> retz.getFile(id, Objects.requireNonNull(file), offset, length));
    }

    public Pair<Integer, byte[]> getBinaryFile(int id, String file) throws IOException {
        String date = TimestampHelper.now();
        String resource = "/job/" + id + "/download?path=" + file;
        AuthHeader header = authenticator.header("GET", "", date, resource);
        URL url = new URL(uri.getScheme() + "://" + uri.getHost() + ":" + uri.getPort() + resource); // TODO url-encode!
        LOG.info("Fetching {}", url);
        HttpURLConnection conn;

        conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("GET");
        conn.setRequestProperty("Accept", "application/octet-stream");
        conn.setRequestProperty("Authorization", header.buildHeader());
        conn.setRequestProperty("Date", date);
        conn.setRequestProperty("Content-md5", "");
        conn.setDoInput(true);
        String s2s = authenticator.string2sign("GET", "", date, resource);
        LOG.debug("Authorization: {} / S2S={}", header.buildHeader(), s2s);

        if (conn.getResponseCode() != 200) {
            if (conn.getResponseCode() < 200) {
                throw new AssertionError(conn.getResponseMessage());
            } else if (conn.getResponseCode() < 300) {
                return new Pair<>(conn.getResponseCode(), "".getBytes(UTF_8)); // Mostly 204; success
            } else if (conn.getResponseCode() < 400) {
                return new Pair<>(conn.getResponseCode(), conn.getResponseMessage().getBytes(UTF_8));
            } else if (conn.getResponseCode() == 404) {
                throw new FileNotFoundException(url.toString());
            } else {
                return new Pair<>(conn.getResponseCode(), conn.getResponseMessage().getBytes(UTF_8));
            }
        }

        int size = conn.getContentLength();
        if (size < 0) {
            throw new IOException("Illegal content length:" + size);
        } else if (size == 0) {
            // not bytes to save;
            return new Pair<>(conn.getResponseCode(), new byte[0]);
        } else if (size > MAX_BIN_SIZE) {
            throw new IOException("Download file too large: " + size);
        }
        byte[] buffer = new byte[size];
        try (BufferedInputStream in = new BufferedInputStream(conn.getInputStream())) {

            int offset = 0;
            while (offset < size) {
                int read = in.read(buffer, offset, size - offset);
                if (read < 0) {
                    break;
                }
                offset += read;
            }
            return new Pair<>(conn.getResponseCode(), buffer);
        } finally {
            conn.disconnect();
        }
    }

    public Response listFiles(int id, String path) throws IOException {
        return Retz.tryOrErrorResponse(
                () -> retz.listFiles(id, Objects.requireNonNull(path)));
    }

    public Job run(Job job) throws IOException {
        Response res = schedule(job);
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
            Response res = getJob(job.id());
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
        return Retz.tryOrErrorResponse(() -> retz.kill(id));
    }

    public Response getApp(String appid) throws IOException {
        return Retz.tryOrErrorResponse(() -> retz.getApp(appid));
    }

    public Response load(Application application) throws IOException {
        return Retz.tryOrErrorResponse(() -> retz.load(Objects.requireNonNull(application)));
    }

    public Response listApp() throws IOException {
        return Retz.tryOrErrorResponse(() -> retz.listApp());
    }

    @Deprecated
    public Response unload(String appName) throws IOException {
        return Retz.tryOrErrorResponse(() -> retz.unload(Objects.requireNonNull(appName)));
    }
}
