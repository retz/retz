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

import io.github.retz.grpcgen.*;
import io.github.retz.protocol.converter.Pb2Retz;
import io.github.retz.protocol.converter.Retz2Pb;
import io.github.retz.scheduler.Applications;
import io.grpc.Context;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class RetzServer {
    private static final Logger LOG = LoggerFactory.getLogger(RetzServer.class);

    private int port;
    private Server server;

    public RetzServer(int port) {
        assert 1024 < port && port < 65536;
        this.port = port;
    }

    public void start() throws IOException {
        server = ServerBuilder.forPort(port)
                .intercept(new ServerAuthInterceptor())
                //.addService(ServerInterceptors.intercept(new RetzServerImpl(), new ServerAuthInterceptor()))
                .addService(new RetzServerImpl())
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
            responseObserver.onCompleted();
        }
    }
}
