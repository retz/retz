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
import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import io.github.retz.protocol.converter.Pb2Retz;
import io.github.retz.protocol.converter.Retz2Pb;
import io.github.retz.protocol.data.Application;
import io.github.retz.protocol.data.Job;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.github.retz.grpcgen.*;

public class Client implements Closeable {
    private final ManagedChannel channel;
    private final RetzGrpc.RetzBlockingStub blockingStub;

    public Client(URI uri) {
        this(uri.getHost(), uri.getPort());
        assert uri.getScheme().equals("grpc");
    }

    public Client(String host, int port) {
        this(ManagedChannelBuilder.forAddress(host, port)
                // Channels are secure by default (via SSL/TLS). For the example we disable TLS to avoid
                // needing certificates.
                .usePlaintext(true)
                .build());
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

    public List<Application> listApps() throws Exception {
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

    public List<Job> listJobs(io.github.retz.protocol.data.Job.JobState state,
                              Optional<String> tag) throws Exception {
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
}

