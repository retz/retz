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
import feign.FeignException;
import io.github.retz.auth.AuthHeader;
import io.github.retz.auth.Authenticator;
import io.github.retz.cli.TimestampHelper;
import io.github.retz.protocol.*;
import io.github.retz.protocol.data.Application;
import io.github.retz.protocol.data.Job;
import io.github.retz.protocol.exception.UnknownServerResponseException;
import io.github.retz.web.feign.Retz;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.*;
import java.io.*;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.Objects;
import java.util.Optional;
import java.util.ResourceBundle;

public class Client implements AutoCloseable {

    public static final String VERSION_STRING;
    private static final ObjectMapper MAPPER = new ObjectMapper();

    static final Logger LOG = LoggerFactory.getLogger(Client.class);

    static {
        ResourceBundle labels = ResourceBundle.getBundle("retz-client");
        VERSION_STRING = labels.getString("version");
        MAPPER.registerModule(new Jdk8Module());
    }

    private final Retz retz;
    private boolean verboseLog = false;
    private URI uri;
    private Authenticator authenticator;
    private SSLSocketFactory socketFactory;
    private HostnameVerifier hostnameVerifier;
    private boolean checkCert = true;

    protected Client(URI uri, Authenticator authenticator) {
        this(uri, authenticator, true);
    }

    protected Client(URI uri, Authenticator authenticator, boolean checkCert) {
        this.uri = Objects.requireNonNull(uri);
        this.authenticator = Objects.requireNonNull(authenticator);
        this.checkCert = checkCert;
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

    // @return int content size written to OutputStream
    public int getBinaryFile(int id, String file, OutputStream out) throws IOException {
        String date = TimestampHelper.now();
        // Encode path forcibly since we return decoded path by list files
        String encodedFile = file;
        try {
            encodedFile = URLEncoder.encode(file, StandardCharsets.UTF_8.toString());
        } catch (UnsupportedEncodingException e) {
        }
        String resource = "/job/" + id + "/download?path=" + encodedFile;
        AuthHeader header = authenticator.header("GET", "", date, resource);
        URL url = new URL(uri.getScheme() + "://" + uri.getHost() + ":" + uri.getPort() + resource); // TODO url-encode!
        LOG.info("Fetching {}", url);
        HttpURLConnection conn;

        conn = (HttpURLConnection) url.openConnection();
        //LOG.info("classname> {}", conn.getClass().getName());
        if (uri.getScheme().equals("https") && !checkCert && conn instanceof HttpsURLConnection) {
            if (verboseLog) {
                LOG.warn("DANGER ZONE: TLS certificate check is disabled. Set 'retz.tls.insecure = false' at config file to supress this message.");
            }
            HttpsURLConnection sslCon = (HttpsURLConnection) conn;
            if (socketFactory != null) {
                sslCon.setSSLSocketFactory(socketFactory);
            }
            if (hostnameVerifier != null) {
                sslCon.setHostnameVerifier(hostnameVerifier);
            }
        }
        conn.setRequestMethod("GET");
        conn.setRequestProperty("Accept", "application/octet-stream");
        conn.setRequestProperty("Authorization", header.buildHeader());
        conn.setRequestProperty("Date", date);
        conn.setRequestProperty("Content-md5", "");
        conn.setDoInput(true);
        String s2s = authenticator.string2sign("GET", "", date, resource);
        LOG.debug("Authorization: {} / S2S={}", header.buildHeader(), s2s);

        if (conn.getResponseCode() != 200) {
            if (verboseLog) {
                LOG.warn("HTTP Response:", conn.getResponseMessage());
            }
            if (conn.getResponseCode() < 200) {
                throw new AssertionError(conn.getResponseMessage());
            } else if (conn.getResponseCode() == 404) {
                throw new FileNotFoundException(url.toString());
            } else {
                String message;
                try {
                    Response response = MAPPER.readValue(conn.getErrorStream(), Response.class);
                    message = response.status();
                    LOG.error(message, response);
                } catch (JsonProcessingException e) {
                    message = e.toString();
                    LOG.error(message, e);
                }
                throw new UnknownServerResponseException(message);
            }
        }

        int size = conn.getContentLength();
        if (size < 0) {
            throw new IOException("Illegal content length:" + size);
        } else if (size == 0) {
            // not bytes to save;
            return 0;
        }
        try {
            return IOUtils.copy(conn.getInputStream(), out);
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
