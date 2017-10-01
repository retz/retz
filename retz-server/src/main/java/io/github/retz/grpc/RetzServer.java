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

import java.io.IOException;
import io.github.retz.grpcgen.RetzGrpc;
import io.github.retz.grpcgen.*;
import io.grpc.stub.StreamObserver;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
                .addService(new RetzServerImpl())
                //.intercept(new KnapsackAuth())
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

    static class RetzServerImpl extends RetzGrpc.RetzImplBase {
        @Override
        public void ping(PingRequest request, StreamObserver<PingResponse> responseObserver) {
            LOG.info("Request (ping)");
            PingResponse response = PingResponse.newBuilder().setPong(true).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
    }
}
