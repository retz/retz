/**
 *    Retz
 *    Copyright (C) 2016 Nautilus Technologies, KK.
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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/*
 * COMMENT: I know it's quite ugly :S and I"ll leave as it is for now
 *  1. Removed
 *  2. Message is not just a String, but has structured data like Job and Application
 *     (see com.asakusafw.retz.web.protocol or doc/ja/retz.md) - reasons for WebSocket
 *     and not simple HTTP consists of some:
 *     a. some APIs are asynchronous; remarkably thinking of use case of sending 'schedule'
 *        and waiting for its update like started/finished/failed - which cannot be done
 *        just with a pair of call/response.
 *     b. Also, which API is gonna be sync/async was not sure when I started
 *     c. Cost of TCP connection initiation and authentication, maintaining single TCP
 *        connection could be cheaper?
 *     d. WebSocket is the way cooler than vanilla HTTP and GUI is nice.
 *
 * REVIEW: It seems so complex.
 *   I think this only has three main features:
 *     1. session lifecycle management (onConnect/onClose)
 *       I think 'awaitConnect()' method and 'connectLatch' can be removed.
 *       WebSocketClient.connect() returns Future<Session>, and it provides await function as Future.get().
 *     2. synchronous RPC
 *       If each message is not identical (or no IDs), then I think just 'send(message:String):String' is enough.
 *       This sends message and just wait for response.
 *     3. asynchronous broadcast RPC result to subscriber(s)
 *       I think separating this class into RPC and subscribe instead if they are independent use cases.
 *       If you want to only subscribe messages, you can just use LinkedBlockingQueue<String> instead of
 *       the pair of CountDownLatch and bare String.
 */
@WebSocket(
        maxTextMessageSize = Connection.MAX_PAYLOAD_SIZE,
        maxIdleTime = Connection.IDLE_TIMEOUT_SEC * 1000)
public class MySocket {
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

    public String awaitResponse(int timeoutSec) throws InterruptedException, TimeoutException {
        if (requestLatch.await(timeoutSec, TimeUnit.SECONDS)) {
            synchronized (this) {
                requestLatch = new CountDownLatch(1);
                return response;
            }
        }
        throw new TimeoutException();
    }

    public synchronized boolean sendRequest(String msg) throws IOException {
        if (this.requestLatch.getCount() > 0) {
            session.getRemote().sendString(msg);
            return true;
        } else {
            // Request is being sent or the socket is already closed
            return false;
        }
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
                        session.getRemoteAddress().getHostName(), error.getMessage());
                response = String.format("{\"status\":\"%s\", \"command\":\"error\"}", msg);
            } else {
                response = String.format("{\"status\":\"%s\", \"command\":\"error\"}", error.getMessage());
            }
        }
        requestLatch.countDown();
    }

    @OnWebSocketFrame
    public void onSocketFrame(Session user, Frame frame) throws IOException {
        if (frame.getType() == Frame.Type.PING) {
            // System.err.println("PING from " + user.getRemoteAddress());
            ByteBuffer bb = ByteBuffer.wrap("pong".getBytes(StandardCharsets.UTF_8));
            user.getRemote().sendPong(bb);
        } else if (frame.getType() == Frame.Type.PONG) {
            // OK
        }
    }

    @OnWebSocketMessage
    public void onMessage(String msg) {
        synchronized (this) {
            response = msg;
        }
        requestLatch.countDown();
    }
}