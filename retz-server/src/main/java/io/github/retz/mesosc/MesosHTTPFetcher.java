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
package io.github.retz.mesosc;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.retz.misc.Pair;
import io.github.retz.misc.Receivable;
import io.github.retz.misc.Triad;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.*;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * A fetcher class that fetches various information from Mesos masters and slaves
 */
public final class MesosHTTPFetcher {
    private static final Logger LOG = LoggerFactory.getLogger(MesosHTTPFetcher.class);
    private static final int RETRY_LIMIT = 3;

    private MesosHTTPFetcher() {
    }

    public static Optional<String> sandboxBaseUri(String master, String slaveId,
                                                  String frameworkId, String executorId,
                                                  String containerId) {
        return sandboxUri("browse", master, slaveId, frameworkId, executorId, containerId, RETRY_LIMIT);
    }

    public static Optional<String> sandboxDownloadUri(String master, String slaveId,
                                                      String frameworkId, String executorId,
                                                      String containerId, String path) {
        Optional<String> base = sandboxUri("download", master, slaveId, frameworkId, executorId, containerId, RETRY_LIMIT);
        if (base.isPresent()) {
            try {
                String encodedPath = URLEncoder.encode(path, UTF_8.toString());
                return Optional.of(base.get() + encodedPath);
            } catch (UnsupportedEncodingException e) {
                return Optional.empty();
            }
        }
        return Optional.empty();
    }

    public static Optional<String> sandboxUri(String t, String master, String slaveId,
                                              String frameworkId, String executorId,
                                              String containerId, int retryRemain) {
        if (retryRemain > 0) {
            Optional<String> maybeUri = sandboxUri(t, master, slaveId, frameworkId, executorId, containerId);
            if (maybeUri.isPresent()) {
                return maybeUri;
            } else {
                // NOTE: this sleep is so short because this function may be called in the context of
                // Mesos Scheduler API callback
                // TODO: sole resolution is to remove all these uri fetching but to build it of Job information, including SlaveId
                LOG.warn("{} retry happening for frameworkId={}, executorId={}, containerId={}", RETRY_LIMIT - retryRemain + 1, frameworkId, executorId, containerId);
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                }
                return sandboxUri(t, master, slaveId, frameworkId, executorId, containerId, retryRemain - 1);
            }
        } else {
            LOG.error("{} retries on fetching sandbox URI failed", RETRY_LIMIT);
            return Optional.empty();
        }
    }

    // slave-hostname:5051/files/download?path=/tmp/mesos/slaves/<slaveid>/frameworks/<frameworkid>/exexutors/<executorid>/runs/<containerid>
    public static Optional<String> sandboxUri(String t, String master, String slaveId,
                                              String frameworkId, String executorId,
                                              String containerId) {
        Optional<String> slaveAddr = fetchSlaveAddr(master, slaveId); // get master:5050/slaves with slaves/pid, cut with '@'
        LOG.debug("Agent address of executor {}: {}", executorId, slaveAddr);

        if (!slaveAddr.isPresent()) {
            return Optional.empty();
        }

        Optional<String> dir = fetchDirectory(slaveAddr.get(), frameworkId, executorId, containerId);
        if (!dir.isPresent()) {
            return Optional.empty();
        }

        try {
            return Optional.of(String.format("http://%s/files/%s?path=%s",
                    slaveAddr.get(), t,
                    URLEncoder.encode(dir.get(), //builder.toString(),
                            UTF_8.toString())));
        } catch (UnsupportedEncodingException e) {
            LOG.error(e.toString(), e);
            return Optional.empty();
        }
    }

    private static Optional<String> fetchSlaveAddr(String master, String slaveId) {
        String addr = "http://" + master + "/slaves";
        try (UrlConnector conn = new UrlConnector(addr, "GET", true)) {
            return extractSlaveAddr(conn.getInputStream(), slaveId);
        } catch (MalformedURLException e) {
            LOG.error(e.toString(), e);
            return Optional.empty();
        } catch (IOException e) {
            LOG.warn("Failed to fetch Slave address of {} from master {}", slaveId, master, e);
            return Optional.empty();
        }
    }

    public static Optional<String> extractSlaveAddr(InputStream stream, String slaveId) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        Map<String, List<Map<String, Object>>> map = mapper.readValue(stream, java.util.Map.class);
        String pid;
        for (Map<String, Object> slave : map.get("slaves")) {
            if (slave.get("id").equals(slaveId)) {
                pid = (String) slave.get("pid");
                String[] tokens = pid.split("@");
                assert tokens.length == 2;
                return Optional.of(tokens[1]);
            }
        }
        return Optional.empty();
    }

    public static Optional<String> extractSlaveBasePath(InputStream stream) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        Map<String, Map<String, Object>> map = mapper.readValue(stream, java.util.Map.class);
        String dir = (String) map.get("flags").get("work_dir");

        return Optional.of(dir);
    }


    private static Optional<String> fetchDirectory(String slave, String frameworkId,
                                                   String executorId, String containerId) {
        String addr = "http://" + slave + "/state";
        try (UrlConnector conn = new UrlConnector(addr, "GET", true)) {
            return extractDirectory(conn.getInputStream(), frameworkId, executorId, containerId);
        } catch (IOException e) {
            LOG.warn("Failed to fetch directory of Slave {} (framework={}, executor={})",
                    slave, frameworkId, executorId, e);
            return Optional.empty();
        }
    }

    public static Optional<String> extractDirectory(InputStream stream, String frameworkId,
                                                    String executorId, String containerId) throws IOException {
        // TODO: prepare corresponding object type instead of using java.util.Map
        // Search path: {frameworks|complated_frameworks}/{completed_executors|executors}[.container='containerId'].directory
        ObjectMapper mapper = new ObjectMapper();
        Map<String, Map<String, Object>> map = mapper.readValue(stream, java.util.Map.class);
        List<Map<String, Object>> frameworks = new ArrayList<>();
        if (map.get("frameworks") != null) {
            frameworks.addAll((List) map.get("frameworks"));
        }
        if (map.get("completed_frameworks") != null) {
            frameworks.addAll((List) map.get("completed_frameworks"));
        }

        // TODO: use json-path for cleaner and flexible code
        for (Map<String, Object> framework : frameworks) {
            List<Map<String, Object>> both = new ArrayList<>();
            List<Map<String, Object>> executors = (List) framework.get("executors");
            if (executors != null) {
                both.addAll(executors);
            }
            List<Map<String, Object>> completedExecutors = (List) framework.get("completed_executors");
            if (completedExecutors != null) {
                both.addAll(completedExecutors);
            }
            Optional<String> s = extractDirectoryFromExecutors(both, executorId, containerId);
            if (s.isPresent()) {
                return s;
            }
        }
        LOG.error("No matching directory at framework={}, executor={}, container={}", frameworkId, executorId, containerId);
        return Optional.empty();
    }

    private static Optional<String> extractDirectoryFromExecutors(List<Map<String, Object>> executors,
                                                                  String executorId, String containerId) {
        for (Map<String, Object> executor : executors) {
            if (executorId.equals(executor.get("id")) && containerId.equals(executor.get("container"))) {
                // TODO: verify frameworkId
                return Optional.ofNullable((String) executor.get("directory"));
            }
        }
        return Optional.empty();
    }

    public static List<Map<String, Object>> fetchTasks(String master, String frameworkId, int offset, int limit) throws MalformedURLException {
        String addr = "http://" + master + "/tasks?offset=" + offset + "&limit=" + limit;
        try (UrlConnector conn = new UrlConnector(addr, "GET", true)) {
            return parseTasks(conn.getInputStream(), frameworkId);
        } catch (IOException e) {
            LOG.error(e.toString(), e);
            return Collections.emptyList();
        }
    }

    public static List<Map<String, Object>> parseTasks(InputStream in, String frameworkId) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        List<Map<String, Object>> ret = new ArrayList<>();

        Map<String, List<Map<String, Object>>> map = mapper.readValue(in, Map.class);
        List<Map<String, Object>> tasks = map.get("tasks");
        for (Map<String, Object> task : tasks) {
            String fid = (String) task.get("framework_id");
            if (!frameworkId.equals(fid)) {
                continue;
            }
            ret.add(task);
        }
        return ret;
    }

    public static void downloadHTTPFile(String url, String name, Receivable<Triad<Integer, String, Pair<Long, InputStream>>, Exception> cb) throws Exception {
        String addr = url.replace("files/browse", "files/download") + "%2F" + maybeURLEncode(name);
        LOG.debug("Downloading {}", addr);
        try (UrlConnector conn = new UrlConnector(addr, "GET")) {
            Integer statusCode = conn.getResponseCode();
            String message = conn.getResponseMessage();
            Long length = conn.getHeaderFieldLong("Content-Length", -1);
            if (LOG.isDebugEnabled()) {
                LOG.debug("res={}, md5={}, length={}", message, conn.getHeaderField("Content-md5"), length);
            }
            cb.receive(new Triad<>(statusCode, message, new Pair<>(length, conn.getInputStream())));
        }
    }


    // Actually Mesos 1.1.0 returns whole body even if it gets HEAD request.
    public static boolean statHTTPFile(String url, String name) {
        String addr = url.replace("files/browse", "files/download") + "%2F" + maybeURLEncode(name);
        try (UrlConnector conn = new UrlConnector(addr, "HEAD", false)) {
            if (LOG.isDebugEnabled()) {
                LOG.debug(conn.getResponseMessage());
            }
            int statusCode = conn.getResponseCode();
            return statusCode == 200 || statusCode == 204;
        } catch (IOException e) {
            LOG.error("Failed to fetch {}: {}", addr, e.toString(), e);
            return false;
        }
    }

    // Only for String contents
    private static Pair<Integer, String> fetchHTTP(String addr, int retry) throws FileNotFoundException, IOException {
        block:
        try (UrlConnector conn = new UrlConnector(addr, "GET", true)) {
            int statusCode = conn.getResponseCode();
            String message = conn.getResponseMessage();
            if (LOG.isDebugEnabled()) {
                LOG.debug("{} {} for {}", statusCode, message, addr);
            }
            if (statusCode != 200) {
                if (statusCode < 200) {
                    break block; // retry
                }

                LOG.warn("Non-200 status {} returned from Mesos '{}' for GET {}",
                        statusCode, message, addr);
                if (statusCode < 300) {
                    return new Pair<>(statusCode, ""); // Mostly 204; success
                } else if (statusCode < 400) {
                    // TODO: Mesos master failover
                    return new Pair<>(statusCode, message);
                } else if (statusCode == 404) {
                    throw new FileNotFoundException(addr);
                } else {
                    return new Pair<>(statusCode, message);
                }
            }

            try (InputStream in = conn.getInputStream();
                 ByteArrayOutputStream out = new ByteArrayOutputStream()) {
                long toRead = conn.getContentLength();
                if (toRead < 0) {
                    LOG.warn("Content length is not known ({}) on getting {}", toRead, addr);
                    IOUtils.copy(in, out);
                    return new Pair<>(statusCode, out.toString(UTF_8.toString()));
                }
                long read = IOUtils.copyLarge(in, out, 0, toRead);

                if (read < toRead) {
                    LOG.warn("Unexpected EOF at {}/{} getting {}", read, toRead, addr);
                    break block;
                }
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Fetched {} bytes from {}", read, addr);
                }
                return new Pair<>(statusCode, out.toString(UTF_8.toString()));

            } catch (FileNotFoundException e) {
                throw e;
            } catch (IOException e) {
                // Somehow this happens even HTTP was correct
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Cannot fetch file {}: {}", addr, e.toString());
                }
                // Just retry until your stack get stuck; thanks to SO:33340848
                // and to that crappy HttpURLConnection
                if (retry < 0) {
                    LOG.error("Retry failed. Last error was: {}", e.toString());
                    throw e;
                }
                break block; // retry
            }
        }
        return fetchHTTP(addr, retry - 1);
    }

    public static Pair<Integer, String> fetchHTTPFile(String url, String name, long offset, long length) throws FileNotFoundException, IOException {
        String addr = url.replace("files/browse", "files/read") + "%2F" + maybeURLEncode(name)
                + "&offset=" + offset + "&length=" + length;
        return fetchHTTP(addr, RETRY_LIMIT);
    }

    public static Pair<Integer, String> fetchHTTPDir(String url, String path) throws FileNotFoundException, IOException {
        // Just do 'files/browse and get JSON
        String addr = url + "%2F" + maybeURLEncode(path);
        return fetchHTTP(addr, RETRY_LIMIT);
    }

    private static String maybeURLEncode(String file) {
        try {
            return URLEncoder.encode(file, UTF_8.toString());
        } catch (UnsupportedEncodingException e) {
            return file;
        }
    }

    static class UrlConnector implements Closeable {

        private HttpURLConnection conn;

        UrlConnector(String addr, String method) throws IOException {
            URL url = new URL(addr);
            this.conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod(method);
        }

        UrlConnector(String addr, String method, boolean dooutput) throws IOException {
            this(addr, method);
            conn.setDoOutput(dooutput);
        }

        public int getResponseCode() throws IOException {
            return conn.getResponseCode();
        }

        public String getResponseMessage() throws IOException {
            return conn.getResponseMessage();
        }

        public String getHeaderField(String name) {
            return conn.getHeaderField(name);
        }

        public long getHeaderFieldLong(String name, long defaultValue) {
            return conn.getHeaderFieldLong(name, defaultValue);
        }

        public long getContentLength() {
            return conn.getContentLengthLong();
        }

        public InputStream getInputStream() throws IOException {
            try {
                return conn.getInputStream();
            } catch (IOException e) {
                Integer statusCode;
                try {
                    statusCode = conn.getResponseCode();
                } catch (IOException e1) {
                    LOG.debug("getInputStream.getResponseCode", e1);
                    statusCode = null;
                }
                String field0 = conn.getHeaderField(0);
                LOG.warn("getInputStream exception={}, responseCode={}, headerField0={}", e.toString(), statusCode,
                        field0);
                throw e;
            }
        }

        @Override
        public void close() {
            if (conn != null) {
                conn.disconnect();
                conn = null;
            }
        }
    }
}