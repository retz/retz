package io.github.retz.web.master;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import io.github.retz.protocol.*;
import io.github.retz.protocol.data.Application;
import io.github.retz.protocol.data.Job;
import io.github.retz.protocol.data.MesosContainer;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.client.ClientUpgradeRequest;
import org.eclipse.jetty.websocket.client.WebSocketClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.function.Predicate;


public class Client implements AutoCloseable {
    static final Logger LOG = LoggerFactory.getLogger(Client.class);
    private final ObjectMapper mapper;
    private final String host;
    private final int port;
    private WebSocketClient wsclient = null;
    private MySocket socket = null;

    public Client(String host, int port) {
        this.host = host;
        this.port = port;
        this.mapper = new ObjectMapper();
        this.mapper.registerModule(new Jdk8Module());
    }

    private boolean connect() throws IOException, ExecutionException, URISyntaxException, Exception {
        URI uri = new URI(String.format("ws://%s:%d/cui", host, port));
        this.socket = new MySocket();
        this.wsclient = new WebSocketClient();
        wsclient.start();

        Future<Session> f = wsclient.connect(socket, uri, new ClientUpgradeRequest());
        while (true) {
            try {
                f.get();
                break;
            } catch (InterruptedException e) {
            }
        }
        return !f.isCancelled();
    }

    @Override
    public void close() {
        disconnect();
    }

    public void disconnect() {
        if (wsclient != null) {
            try {
                wsclient.stop();
            } catch (Exception e) {
            }
        }
    }

    public boolean ping() throws IOException {
        URL url;
        try {
            // TODO: make this switchable on http/https
            url = new URL("http://" + host + ":" + port + "/ping");
            LOG.debug("Pinging {}", url);
        } catch (MalformedURLException e) {
            return false;
        }
        HttpURLConnection conn = null;
        try {
            conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");
            conn.setDoOutput(false);
            byte[] buffer = {'n', 'g'};
            int s = conn.getInputStream().read(buffer, 0, 2);
            if (s < 0) {
                return false;
            }
            String msg = new String(buffer, StandardCharsets.UTF_8);
            LOG.info(msg);
            return "OK" .equals(msg);
        } catch (IOException e) {
            LOG.debug(e.toString());
            return false;
        } finally {
            if (conn != null) {
                conn.disconnect();
            }
        }
    }

    public Response listApp() throws IOException {
	return rpc(new ListAppRequest());
    }
    
    private <ReqType extends Request> Response rpc(ReqType req) throws IOException {
        URL url;
        try {
            // TODO: make this switchable on http/https
            url = new URL("http://" + host + ":" + port + req.resource());
            LOG.debug("Connecting {}", url);
        } catch (MalformedURLException e) {
            return new ErrorResponse(e.toString());
        }
        HttpURLConnection conn = null;
        try {
            conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod(req.method());
            conn.setDoOutput(req.hasPayload());
            if (req.hasPayload()) {
                mapper.writeValue(conn.getOutputStream(), req);
            }
            return mapper.readValue(conn.getInputStream(), Response.class);
        } catch (IOException e) {
            LOG.debug(e.toString());
            return new ErrorResponse(e.toString());
        } finally {
            if (conn != null) {
                conn.disconnect();
            }
        }
    }

    // Wait for good response until the callback gives non-null good value.
    // To keep watching, callback must keep returning null.
    public <RetType> RetType waitForResponse(Function<WatchResponse, RetType> callback) throws IOException {
        try {
            if (!connect()) {
                LOG.error("failed to connect to host");
                return null;
            } else {
                LOG.info("Connected to the host");
            }
        } catch (Exception e) {
            LOG.error(e.toString());
            return null;
        }
        RetType ret = null;
        while (ret == null) {
            String json;
            try {
                json = socket.awaitResponse();
                LOG.info(json);
            } catch (InterruptedException e) {
                continue;
            }
            Response res = mapper.readValue(json, Response.class);
            if (res instanceof WatchResponse) {
                WatchResponse wres = (WatchResponse) res;
                ret = callback.apply(wres);
            } else if (res instanceof ErrorResponse) {
                ErrorResponse eres = (ErrorResponse) res;
                LOG.error(eres.status());
                return null;
            }
        }
        return ret;
    }

    // This method does block while callback returns.
    // The callback must return true to keep watching.
    public void startWatch(Predicate<WatchResponse> callback) throws IOException {
        waitForResponse(watchResponse -> {
            if (callback.test(watchResponse)) {
                return null;
            } else {
                return 1;
            }
        });

    }
}
    
