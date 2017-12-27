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

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import io.github.retz.auth.Authenticator;
import io.github.retz.protocol.converter.Pb2Retz;
import io.github.retz.protocol.converter.Retz2Pb;
import io.github.retz.protocol.data.Application;
import io.github.retz.protocol.data.DirEntry;
import io.github.retz.protocol.data.Job;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.github.retz.grpcgen.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Client implements Closeable {
    static final Logger LOG = LoggerFactory.getLogger(Client.class);

    private final ManagedChannel channel;
    private final RetzGrpc.RetzBlockingStub blockingStub;

    private Authenticator authenticator;

    public Client(URI uri, Authenticator authenticator) {
        this(uri.getHost(), uri.getPort(), authenticator);
        assert uri.getScheme().equals("grpc");
    }

    public Client(String host, int port, Authenticator authenticator) {
        this(ManagedChannelBuilder.forAddress(host, port)
                // Channels are secure by default (via SSL/TLS). For the example we disable TLS to avoid
                // needing certificates.
                .usePlaintext(true)
                .intercept(new AuthHeaderInterceptor(authenticator))
                .build());
        this.authenticator = authenticator;
        LOG.info("Connecting to {}:{}", host, port);
    }

    Client(ManagedChannel channel) {
        this.channel = channel;
        blockingStub = RetzGrpc.newBlockingStub(channel);
    }

    public void close() {
        try {
            channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
        }
    }

    public boolean ping() throws Exception {
        PingRequest request = PingRequest.newBuilder().build();
        PingResponse response = blockingStub.ping(request);
        return response.getPong();
    }

    public List<Application> listApps() {
        ListAppRequest request = ListAppRequest.newBuilder().build();
        Iterator<ListAppResponse> responses = blockingStub.listApp(request);

        List<Application> apps = new ArrayList<>();
        while(responses.hasNext()) {
            ListAppResponse response = responses.next();
            apps.addAll(response.getAppsList().stream()
            .map(gApp -> Pb2Retz.convert(gApp))
            .collect(Collectors.toList()));
        }
        return apps;
    }

    public void loadApp(Application application) {
        LoadAppRequest request = LoadAppRequest.newBuilder()
                .setApp(Retz2Pb.convert(application))
                .build();
        LoadAppResponse res = blockingStub.loadApp(request);
        LOG.debug(res.getError());
    }

    public Application getApp(String name) {
        GetAppRequest request = GetAppRequest.newBuilder()
                .setName(name)
                .build();
        GetAppResponse res = blockingStub.getApp(request);
        return Pb2Retz.convert(res.getApp());
    }

    public List<Job> listJobs(io.github.retz.protocol.data.Job.JobState state,
                              Optional<String> tag) {
        ListJobRequest.Builder builder = ListJobRequest.newBuilder();
        builder.setState(Retz2Pb.convert(state));
        if (tag.isPresent()) {
            builder.setTag(tag.get());
        }
        Iterator<ListJobResponse> responses = blockingStub.listJob(builder.build());

        List<Job> jobs = new ArrayList<>();
        while (responses.hasNext()) {
            ListJobResponse response = responses.next();
            jobs.addAll(response.getJobsList().stream()
            .map(gJob -> Pb2Retz.convert(gJob))
            .collect(Collectors.toList()));
        }
        return jobs;
    }

    public Optional<Job> schedule(Job job) {
        ScheduleRequest request = ScheduleRequest.newBuilder()
                .setJob(Retz2Pb.convert(job)).build();
        ScheduleResponse res = blockingStub.schedule(request);
        if (! res.hasJob()) {
            // TODO: how can we know is error is retryable or not?
            LOG.error(res.getError());
            return Optional.empty();
        }
        return  Optional.ofNullable(Pb2Retz.convert(res.getJob()));
    }

    public Optional<Job> getJob(int id) {
        GetJobRequest request = GetJobRequest.newBuilder()
                .setId(id)
                .build();
        GetJobResponse res = blockingStub.getJob(request);
        if (res.hasJob()) {
            return Optional.of(Pb2Retz.convert(res.getJob()));
        }
        LOG.debug(res.getError());
        return Optional.empty();
    }

    public void kill(int id) {
        KillRequest request = KillRequest.newBuilder()
                .setId(id).build();
        KillResponse res = blockingStub.kill(request);
        LOG.debug(res.getError());
    }

    public long getFile(int id, String path, OutputStream out) throws IOException {
        return getFile(id, path, 0, -1, out);
    }

    // So far this won't be implemented because server side downloader is not yet ready
    private long getFile(int id, String path, long offset, long length, OutputStream out) throws IOException {
        GetFileRequest request = GetFileRequest.newBuilder()
                .setId(id)
                .setFile(path)
                .setOffset(offset)
                .setLength(length)
                .build();
        Iterator<GetFileResponse> iter = blockingStub.getFile(request);
        long bytes = 0;
        while(iter.hasNext()) {
            GetFileResponse res = iter.next();
            out.write(res.getContent().toByteArray());
            // Not implemented yet; offset will be always 0
            //if (offset + bytes != res.getOffset()) {
            //    LOG.error("offset alignment got wrong: {} + {} != {}", offset, bytes, res.getOffset());
            //}
            bytes += res.getContent().size();
        }
        return bytes;
    }

    public List<DirEntry> listFiles(int id, String path) {
        ListFilesRequest request = ListFilesRequest.newBuilder()
                .setId(id)
                .setPath(path)
                .build();
        Iterator<ListFilesResponse> iter = blockingStub.listFiles(request);
        List<DirEntry> ret = new ArrayList<>();
        while (iter.hasNext()) {
            ListFilesResponse res = iter.next();
            ret.addAll(res.getEntryList().stream().map(Pb2Retz::convert).collect(Collectors.toList()));
        }
        return ret;
    }
}

