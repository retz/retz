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

import java.io.IOException;
import java.net.URI;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.Objects;
import java.util.ResourceBundle;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import feign.FeignException;
import io.github.retz.auth.Authenticator;
import io.github.retz.protocol.GetJobResponse;
import io.github.retz.protocol.Response;
import io.github.retz.protocol.ScheduleResponse;
import io.github.retz.protocol.data.Application;
import io.github.retz.protocol.data.Job;
import io.github.retz.web.feign.Retz;

public class Client implements AutoCloseable {

    public static final String VERSION_STRING;

    static final Logger LOG = LoggerFactory.getLogger(Client.class);

    static {
        ResourceBundle labels = ResourceBundle.getBundle("retz-client");
        VERSION_STRING = labels.getString("version");
    }

    private final Retz retz;
    private boolean verboseLog = false;

    protected Client(URI uri, Authenticator authenticator) {
        this(uri, authenticator, true);
    }

    protected Client(URI uri, Authenticator authenticator, boolean checkCert) {
        SSLSocketFactory socketFactory;
        HostnameVerifier hostnameVerifier;
        if (uri.getScheme().equals("https") && !checkCert) {
            LOG.warn("DANGER ZONE: TLS certificate check is disabled. Set 'retz.tls.insecure = false' at config file to supress this message.");
            try {
                SSLContext sc = SSLContext.getInstance("SSL");
                sc.init(null, new TrustManager[] { new WrongTrustManager() }, new java.security.SecureRandom());
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

    public void setVerboseLog(boolean b) {
        verboseLog = b;
    }

    public static ClientBuilder newBuilder(URI uri) {
        return new ClientBuilder(uri);
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

    public Response list(int limit) throws IOException {
        return Retz.tryOrErrorResponse(() -> retz.list());
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
