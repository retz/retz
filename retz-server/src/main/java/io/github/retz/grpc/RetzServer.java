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
package io.github.retz.grpc;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.google.protobuf.ByteString;
import io.github.retz.cli.TimestampHelper;
import io.github.retz.db.Database;
import io.github.retz.grpcgen.*;
import io.github.retz.mesosc.MesosHTTPFetcher;
import io.github.retz.misc.Pair;
import io.github.retz.misc.Triad;
import io.github.retz.planner.AppJobPair;
import io.github.retz.protocol.converter.Pb2Retz;
import io.github.retz.protocol.converter.Retz2Pb;
import io.github.retz.protocol.exception.DownloadFileSizeExceeded;
import io.github.retz.scheduler.Applications;
import io.github.retz.scheduler.JobQueue;
import io.github.retz.scheduler.ServerConfiguration;
import io.github.retz.scheduler.Stanchion;
import io.grpc.Context;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

public class RetzServer {
    private static final Logger LOG = LoggerFactory.getLogger(RetzServer.class);

    private int port;
    private Server server;
    private ServerConfiguration config;

    int maxListJobSize;

    public RetzServer(ServerConfiguration config) {
        this.config = Objects.requireNonNull(config);
        this.port = config.getUri().getPort();
    }

    public RetzServer(int port) {
        assert 1024 < port && port < 65536;
        this.port = port;
    }

    public void start() throws IOException {
        server = ServerBuilder.forPort(port)
                .intercept(new ServerAuthInterceptor())
                //.addService(ServerInterceptors.intercept(new RetzServerImpl(), new ServerAuthInterceptor()))
                .addService(new RetzServerImpl(this.config))
                .build()
                .start();
        LOG.info("Server started, listening on " + port);

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                // Use stderr here since the logger may have been reset by its JVM shutdown hook.
                System.err.println("*** shutting down gRPC server since JVM is shutting down");
                RetzServer.this.stop();
                System.err.println("*** server shut down");
            }
        });
    }

    public void stop() {
        if (server != null) {
            server.shutdown();
        }
    }

    public void blockUntilShutdown() {
        while (server != null) {
            try {
                server.awaitTermination();
                server = null;
                break;
            } catch (InterruptedException e) {
                continue;
            }
        }
    }
    public static final Context.Key<String> USER_ID_KEY = Context.key("userId");
    static class RetzServerImpl extends RetzGrpc.RetzImplBase {

        ServerConfiguration config;
        int maxListJobSize;

        public RetzServerImpl(ServerConfiguration config) {
            this.config = config;
            this.maxListJobSize =config.getMaxListJobSize();
        }

        @Override
        public void ping(PingRequest request, StreamObserver<PingResponse> responseObserver) {
            LOG.info("Request (ping)");
            PingResponse response = PingResponse.newBuilder().setPong(true).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        @Override
        public void listApp(ListAppRequest request, StreamObserver<ListAppResponse> responseObserver) {
            String user = USER_ID_KEY.get(Context.current());
            LOG.info("Key: {}", user);
            try {
                List<Application> applications = Applications.getAll(user).stream().map(application -> Retz2Pb.convert(application)).collect(Collectors.toList());
                ListAppResponse response = ListAppResponse.newBuilder().addAllApps(applications).build();
                responseObserver.onNext(response);
                responseObserver.onCompleted();
            } catch (IOException e) {
                responseObserver.onError(e);
            }
        }

        static void validateOwner(String key, Application app) {
            if (!app.getOwner().equals(key)) {
                LOG.debug("Invalid request: requester and owner does not match");
            }
        }

        @Override
        public void loadApp(LoadAppRequest request, StreamObserver<LoadAppResponse> responseObserver) {
            String user = USER_ID_KEY.get(Context.current());
            LOG.info("Key: {}", user);

            LoadAppResponse.Builder builder = LoadAppResponse.newBuilder();
            io.github.retz.protocol.data.Application application = Pb2Retz.convert(request.getApp());
            if (! application.getOwner().equals(user)) {
                builder.setError("Use name and application owner does not match");
                responseObserver.onNext(builder.build());
                responseObserver.onCompleted();
                return;
            }

            try {
                Optional<io.github.retz.protocol.data.Application> tmp = Applications.get(request.getApp().getAppid());
                if (tmp.isPresent()) {
                    if (!tmp.get().getOwner().equals(user)) {
                        builder.setError("Application is already loaded by different person");
                        responseObserver.onNext(builder.build());
                        responseObserver.onCompleted();
                        return;
                    }
                }

                if (application.container() instanceof io.github.retz.protocol.data.DockerContainer) {
                    if (application.getUser().isPresent()
                            && application.getUser().get().equals("root")) {
                        builder.setError("Cannot be root user at Docker container");
                        responseObserver.onNext(builder.build());
                        responseObserver.onCompleted();
                        return;
                    }
                }

                boolean result = Applications.load(application);
                if (!result) {
                    LOG.error("Failed to load application {}", application.pp());
                }
                responseObserver.onNext(builder.build());
                responseObserver.onCompleted();

            } catch (IOException e) {
                responseObserver.onError(e);
            }
        }

        @Override
        public void getApp(GetAppRequest request, StreamObserver<GetAppResponse> responseObserver) {
            String user = USER_ID_KEY.get(Context.current());
            String appname = request.getName();

            Optional<io.github.retz.protocol.data.Application> maybeApp = Optional.empty();

            try {
                maybeApp = Applications.get(appname);
            } catch (IOException e) {
                LOG.error(e.toString(), e);
                responseObserver.onError(e);
                return;
            }

            GetAppResponse.Builder builder = GetAppResponse.newBuilder();
            if (! maybeApp.isPresent()) {
                builder.setError("No such application:" + appname);
                responseObserver.onNext(builder.build());
                responseObserver.onCompleted();
                return;
            }

            // Compare application owner and requester
            io.github.retz.protocol.data.Application application = maybeApp.get();
            if ( !application.getOwner().equals(user) ) {
                builder.setError(user + " is not the owner of " + appname);
                responseObserver.onNext(builder.build());
                responseObserver.onCompleted();
                return;
            }

            builder.setApp(Retz2Pb.convert(application));
            responseObserver.onNext(builder.build());
            responseObserver.onCompleted();
            return;
        }

        @Override
        public void listJob(ListJobRequest request, StreamObserver<ListJobResponse> responseObserver) {
            String user = Objects.requireNonNull(USER_ID_KEY.get(Context.current()));

            try {
                List<io.github.retz.protocol.data.Job> jobs = JobQueue.list(user,
                        Pb2Retz.convert(request.getState()),
                        Optional.ofNullable(request.getTag()),
                        maxListJobSize);

                ListJobResponse response = ListJobResponse.newBuilder()
                        .addAllJobs(jobs.stream().map(Retz2Pb::convert).collect(Collectors.toList()))
                        .build();

                responseObserver.onNext(response);
                responseObserver.onCompleted();

            } catch (IOException e) {
                responseObserver.onError(e);
            }
        }

        @Override
        public void schedule(ScheduleRequest request, StreamObserver<ScheduleResponse> responseObserver) {
            String user = Objects.requireNonNull(USER_ID_KEY.get(Context.current()));
            ScheduleResponse.Builder builder = ScheduleResponse.newBuilder();

            try {
                Optional<io.github.retz.protocol.data.Application> maybeApp = Applications.get(request.getJob().getAppid()); // TODO check owner right here
                if (!maybeApp.isPresent()) {
                    // TODO: this warn log cannot be written in real stable release
                    LOG.warn("No such application loaded: {}", request.getJob().getAppid());
                    builder.setError("No such application");
                    responseObserver.onNext(builder.build());
                    responseObserver.onCompleted();
                    return;

                }

                if (maybeApp.get().getOwner().equals(user)) {
                    builder.setError("Not an owner of the application");
                    responseObserver.onCompleted();
                    return;
                }

                if (! maybeApp.get().enabled()) {
                    builder.setError("The application is disabled");
                    responseObserver.onCompleted();
                    return;
                }

                io.github.retz.protocol.data.Job job = Pb2Retz.convert(request.getJob());

                if (! config.getMaxJobSize().fits(job) ){
                    String msg = "Job " + job.toString() + " does not fit system limit " + config.getMaxJobSize();
                    // TODO: this warn log cannot be written in real stable release
                    builder.setError(msg);
                    responseObserver.onCompleted();
                    return;
                }

                job.schedule(JobQueue.issueJobId(), TimestampHelper.now());
                JobQueue.push(job);

                // TODO: implement invokeNow
                /**
                 if (scheduler.isPresent() && driver.isPresent()) {
                 LOG.info("Trying invocation from offer stock: {}", job);
                 scheduler.get().maybeInvokeNow(driver.get(), job);
                 } **/

                builder.setJob(Retz2Pb.convert(job));
                responseObserver.onNext(builder.build());
                LOG.info("Job (id={}) '{}' at {} has been scheduled at {}.",
                        job.id(), job.cmd(), job.appid(), job.scheduled());

            } catch (IOException e) {
                responseObserver.onError(e);
            }
        }

        private static Optional<Pair<io.github.retz.protocol.data.Application,
                io.github.retz.protocol.data.Job>> getJobAndVerify(int id, String user) throws IOException {

            Optional<AppJobPair> maybePair = Database.getInstance().getAppJob(id);
            if (maybePair.isPresent()) {
                AppJobPair pair = maybePair.get();
                if (pair.application().getOwner().equals(user)) {
                    return Optional.of(new Pair(pair.application(), pair.job()));
                }
            }
            return Optional.empty();
        }

        @Override
        public void getJob(GetJobRequest request, StreamObserver<GetJobResponse> responseObserver) {
            String user = Objects.requireNonNull(USER_ID_KEY.get(Context.current()));
            int id = request.getId();
            try {
                Optional<Pair<io.github.retz.protocol.data.Application, io.github.retz.protocol.data.Job>> maybePair = getJobAndVerify(id, user);
                GetJobResponse.Builder builder = GetJobResponse.newBuilder();
                if (! maybePair.isPresent()) {
                    builder.setError("No job found");
                    responseObserver.onNext(builder.build());
                    responseObserver.onCompleted();
                    return;
                }
                builder.setJob(Retz2Pb.convert(maybePair.get().right()));
                responseObserver.onNext(builder.build());
                responseObserver.onCompleted();
                return;
            } catch (IOException e) {
                responseObserver.onError(e);
            }
        }

        @Override
        public void kill(KillRequest request, StreamObserver<KillResponse> responseObserver) {
            String user = Objects.requireNonNull(USER_ID_KEY.get(Context.current()));
            int id = request.getId();
            try {
                Optional<Pair<io.github.retz.protocol.data.Application, io.github.retz.protocol.data.Job>> maybePair = getJobAndVerify(id, user);
                KillResponse.Builder builder = KillResponse.newBuilder();
                if (!maybePair.isPresent()) {
                    builder.setError("No job found");
                    responseObserver.onNext(builder.build());
                    responseObserver.onCompleted();
                    return;
                }

                boolean result = Stanchion.call(() -> {
                    Optional<io.github.retz.protocol.data.Job> maybeJob2 = JobQueue.cancel(id, "Canceled by user");

                        if (maybeJob2.isPresent()) {
                            io.github.retz.protocol.data.Job job = maybeJob2.get();
                            // There's a slight pitfall between cancel above and kill below where
                            // no kill may be sent, RetzScheduler is exactly in resourceOffers and being scheduled.
                            // Then this protocol returns false for sure.
                            // TODO: make this work
                            /**
                            if (job.taskId() != null && !job.taskId().isEmpty() && driver.isPresent()) {
                                org.apache.mesos.Protos.TaskID taskId = org.apache.mesos.Protos.TaskID.newBuilder().setValue(job.taskId()).build();
                                Protos.Status status = driver.get().killTask(taskId);
                                LOG.info("Job id={} was running and killed. status={}, taskId={}", job.id(), status, job.taskId());
                            }
                             */
                            return job.state() == io.github.retz.protocol.data.Job.JobState.KILLED;
                        }
                        // Job is already finished or killed, no more running nor runnable, or something is wrong
                        return false;
                    });

                if (!result) {
                    builder.setError("Can't kill job or the job is already killed");
                }
                responseObserver.onNext(builder.build());
                responseObserver.onCompleted();
                return;
            } catch (IOException e) {
                responseObserver.onError(e);
            }
        }

        @Override
        public void getFile(GetFileRequest request, StreamObserver<GetFileResponse> responseObserver) {
            String user = Objects.requireNonNull(USER_ID_KEY.get(Context.current()));
            int id = request.getId();
            String file = request.getFile();
            LOG.debug("download: path={}", file);

            if (request.getOffset() != 0 || request.getLength() != -1) {
                LOG.error("Not yet implemented: offset must be 0 and length must be -1");
                responseObserver.onError(new RuntimeException("not implemented"));
            }

            try {
                Optional<Pair<io.github.retz.protocol.data.Application, io.github.retz.protocol.data.Job>> maybePair = getJobAndVerify(id, user);
                GetFileResponse.Builder builder = GetFileResponse.newBuilder();
                if (! maybePair.isPresent()) {
                    builder.setError("No job found");
                    responseObserver.onNext(builder.build());
                    responseObserver.onCompleted();
                    return;
                }

                // Check file
                io.github.retz.protocol.data.Job job = maybePair.get().right();
                if (job.url() == null) { // If url() is null, the job hasn't yet been started at Mesos / or Bug
                    // TODO: re-fetch job.url() again, because it CAN be null in case of race condition where
                    // updating the url on taskUpdate and master change.
                    if (job.state() != io.github.retz.protocol.data.Job.JobState.CREATED && job.state() != io.github.retz.protocol.data.Job.JobState.STARTING && job.state() != io.github.retz.protocol.data.Job.JobState.QUEUED) {
                        LOG.error("Job (id={}) has its url null (state={})", job.id(), job.state());
                    }
                    // The job has not yet started
                    builder.setError("The job does not have URL (or not yet started)");
                    responseObserver.onNext(builder.build());
                    responseObserver.onCompleted();
                    return;
                }

                // Go download
                MesosHTTPFetcher.downloadHTTPFile(job.url(), file, (Triad<Integer, String, Pair<Long, InputStream>> triad) -> {
                    Integer statusCode = triad.left();

                    if (statusCode == 200) {
                        Long length = triad.right().left();
                        InputStream io = triad.right().right();
                        Long maxFileSize = config.getMaxFileSize();
                        if (length < 0) {
                            throw new IOException("content length is negative: " + length);
                        } else if (0 <= maxFileSize && maxFileSize < length) { // negative maxFileSize indicates no limit
                            throw new DownloadFileSizeExceeded(length, maxFileSize);
                        }

                        ByteArrayOutputStream buf = new ByteArrayOutputStream();
                        IOUtils.copyLarge(io, buf, 0, length);
                        builder.setContent(ByteString.copyFrom(buf.toByteArray()))
                                .setOffset(0)
                                .build();

                    } else {
                        builder.setError(triad.center());
                    }
                });

                responseObserver.onNext(builder.build());
                responseObserver.onCompleted();
                return;

            } catch (Exception e) {
                responseObserver.onError(e);
            }

        }

        @Override
        public void listFiles(ListFilesRequest request, StreamObserver<ListFilesResponse> responseObserver) {
            String user = Objects.requireNonNull(USER_ID_KEY.get(Context.current()));
            int id = request.getId();
            String path = request.getPath();
            LOG.debug("get-path: path={}", path);

            try {
                Optional<Pair<io.github.retz.protocol.data.Application, io.github.retz.protocol.data.Job>> maybePair = getJobAndVerify(id, user);
                ListFilesResponse.Builder builder = ListFilesResponse.newBuilder();
                if (!maybePair.isPresent()) {
                    builder.setError("No job found");
                    responseObserver.onNext(builder.build());
                    responseObserver.onCompleted();
                    return;
                }


                io.github.retz.protocol.data.Job job = maybePair.get().right();

                if (job.url() == null) {
                    builder.setError("Job URL is empty; if the Job state is valid please ask admin");
                    responseObserver.onNext(builder.build());
                    responseObserver.onCompleted();
                    return;
                }

                List<io.github.retz.protocol.data.DirEntry> ret = Collections.emptyList();

                ObjectMapper mapper = new ObjectMapper();
                mapper.registerModule(new Jdk8Module());

                // TODO: this can be done in streaming way
                Pair<Integer, String> maybeJson = MesosHTTPFetcher.fetchHTTPDir(job.url(), path);
                if (maybeJson.left() == 200) {
                    ret = mapper.readValue(maybeJson.right(), new TypeReference<List<io.github.retz.protocol.data.DirEntry>>() {
                    });
                }
                builder.addAllEntry(ret.stream().map(Retz2Pb::convert).collect(Collectors.toList()));
                responseObserver.onNext(builder.build());

            } catch (FileNotFoundException e) {
                responseObserver.onError(e);
            } catch (IOException e) {
                responseObserver.onError(e);
            }
        }
    }
}
