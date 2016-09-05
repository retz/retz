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

import io.github.retz.protocol.Connection;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.annotations.*;
import org.eclipse.jetty.websocket.api.extensions.Frame;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@WebSocket(
        maxTextMessageSize = Connection.MAX_PAYLOAD_SIZE,
        maxIdleTime = Connection.IDLE_TIMEOUT_SEC * 1000)
public class MySocket {
    static final Logger LOG = LoggerFactory.getLogger(MySocket.class);

    private Session session;
    private CountDownLatch requestLatch;
    private String response;

    public MySocket() {
        requestLatch = new CountDownLatch(1);
    }

    public String awaitResponse() throws InterruptedException {
        this.requestLatch.await();
        synchronized (this) {
            this.requestLatch = new CountDownLatch(1);
            return response;
        }
    }

    public String awaitResponse(int timeoutSec) throws TimeoutException {
        while (true) {
            try {
                if (requestLatch.await(timeoutSec, TimeUnit.SECONDS)) {
                    synchronized (this) {
                        requestLatch = new CountDownLatch(1);
                        return response;
                    }
                }
                break;
            } catch (InterruptedException e) {
            }
        }
        throw new TimeoutException();
    }

    @OnWebSocketClose
    public void onClose(int statusCode, String reason) {
        synchronized (this) {
            session.close();
            response = "{\"status\":\"closed\", \"command\":\"error\"}";
        }
        requestLatch.countDown();
    }

    @OnWebSocketConnect
    public void onConnect(Session session) throws IOException {
        synchronized (this) {
            this.session = session;
        }
    }

    @OnWebSocketError
    public void onError(Session session, Throwable error) {
        // COMMENT: Won't be using any logger as this is a CUI tool
        // REVIEW: Logger.*(..., Throwable) will print stacktrace of the Throwable
        synchronized (this) {
            if (session != null) {
                String msg = String.format("Connection error from %s: %s",
                        session.getRemoteAddress().getHostName(), error.toString());
                response = String.format("{\"status\":\"%s\", \"command\":\"error\"}", msg);
            } else {
                response = String.format("{\"status\":\"%s\", \"command\":\"error\"}", error.toString());
            }
        }
        requestLatch.countDown();
    }

    @OnWebSocketFrame
    public void onSocketFrame(Session user, Frame frame) throws IOException {
        if (frame.getType() == Frame.Type.PING) {
            ByteBuffer bb = ByteBuffer.wrap("pong".getBytes(StandardCharsets.UTF_8));
            LOG.debug("Got ping from {}", user.getRemoteAddress().getHostName());
            user.getRemote().sendPong(bb);
        } else if (frame.getType() == Frame.Type.PONG) {
            LOG.debug("Got pong from {}", user.getRemoteAddress().getHostName());
            // OK
        }
    }

    @OnWebSocketMessage
    public void onMessage(String msg) {
        LOG.debug("message: {}", msg);
        synchronized (this) {
            response = msg;
        }
        requestLatch.countDown();
    }
}
