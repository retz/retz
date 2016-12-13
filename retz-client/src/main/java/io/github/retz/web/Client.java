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

import java.io.IOException;
import java.net.URI;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.Objects;
import java.util.ResourceBundle;

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

    private final URI uri;

    private final Authenticator authenticator;

    protected Client(URI uri, Authenticator authenticator) {
        this.uri = uri;
        this.authenticator = authenticator;
        System.setProperty("http.agent", Client.VERSION_STRING);
    }

    protected Client(URI uri, Authenticator authenticator, boolean checkCert) {
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
        try {
            return "OK".equals(Retz.connect(uri, authenticator).ping());
        } catch (FeignException e) {
            LOG.debug(e.toString());
            return false;
        }
    }

    public Response list(int limit) throws IOException {
        return Retz.tryOrErrorResponse(
                () -> Retz.connect(uri, authenticator).list());
    }

    public Response schedule(Job job) throws IOException {
        if (job.priority() < -20 || 19 < job.priority()) {
            throw new IllegalArgumentException("Priority must be [-19, 20]");
        }
        return Retz.tryOrErrorResponse(
                () -> Retz.connect(uri, authenticator)
                        .schedule(Objects.requireNonNull(job)));
    }

    public Response getJob(int id) throws IOException {
        return Retz.tryOrErrorResponse(
                () -> Retz.connect(uri, authenticator).getJob(id));
    }

    public Response getFile(int id, String file, long offset, long length) throws IOException {
        return Retz.tryOrErrorResponse(
                () -> Retz.connect(uri, authenticator)
                        .getFile(id, Objects.requireNonNull(file), offset, length));
    }

    public Response listFiles(int id, String path) throws IOException {
        return Retz.tryOrErrorResponse(
                () -> Retz.connect(uri, authenticator)
                        .listFiles(id, Objects.requireNonNull(path)));
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
        return Retz.tryOrErrorResponse(
                () -> Retz.connect(uri, authenticator).kill(id));
    }

    public Response getApp(String appid) throws IOException {
        return Retz.tryOrErrorResponse(
                () -> Retz.connect(uri, authenticator).getApp(appid));
    }

    public Response load(Application application) throws IOException {
        return Retz.tryOrErrorResponse(
                () -> Retz.connect(uri, authenticator)
                        .load(Objects.requireNonNull(application)));
    }

    public Response listApp() throws IOException {
        return Retz.tryOrErrorResponse(
                () -> Retz.connect(uri, authenticator).listApp());
    }

    @Deprecated
    public Response unload(String appName) throws IOException {
        return Retz.tryOrErrorResponse(
                () -> Retz.connect(uri, authenticator)
                        .unload(Objects.requireNonNull(appName)));
    }
}
