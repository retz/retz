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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * A fetcher class that fetches various information from Mesos masters and slaves
 */
public class MesosHTTPFetcher {
    private static final Logger LOG = LoggerFactory.getLogger(MesosHTTPFetcher.class);

    public static Optional<String> sandboxBaseUri(String master, String slaveId, String frameworkId, String executorId) {
        return sandboxUri("browse", master, slaveId, frameworkId, executorId);
    }

    public static Optional<String> sandboxDownloadUri(String master, String slaveId, String frameworkId, String executorId, String path) {
        Optional<String> base = sandboxUri("download", master, slaveId, frameworkId, executorId);
        if (base.isPresent()) {
            try {
                String encodedPath = java.net.URLEncoder.encode(path, java.nio.charset.StandardCharsets.UTF_8.toString());
                return Optional.of(base.get() + encodedPath);
            } catch (UnsupportedEncodingException e) {
                return Optional.empty();
            }
        }
        return Optional.empty();
    }

    // slave-hostname:5051/files/download?path=/tmp/mesos/slaves/<slaveid>/frameworks/<frameworkid>/exexutors/<executorid>/runs/<containerid>
    public static Optional<String> sandboxUri(String t, String master, String slaveId, String frameworkId, String executorId) {

        Optional<String> slaveAddr = fetchSlaveAddr(master, slaveId); // get master:5050/slaves with slaves/pid, cut with '@'
        LOG.debug("Agent address of executor {}: {}", executorId, slaveAddr);

        if (!slaveAddr.isPresent()) {
            return Optional.empty();
        }

        Optional<String> dir = fetchDirectory(slaveAddr.get(), frameworkId, executorId);
        if (!dir.isPresent()) {
            return Optional.empty();
        }

        try {
            return Optional.of(String.format("http://%s/files/%s?path=%s",
                    slaveAddr.get(), t,
                    java.net.URLEncoder.encode(dir.get(), //builder.toString(),
                            java.nio.charset.StandardCharsets.UTF_8.toString())));
        } catch (UnsupportedEncodingException e) {
            return Optional.empty();
        }
    }

    private static Optional<String> fetchSlaveAddr(String master, String slaveId) {
        URL url;
        try {
            url = new URL("http://" + master + "/slaves");
        } catch (MalformedURLException e) {
            return Optional.empty();
        }
        HttpURLConnection conn;
        try {
            conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");
            conn.setDoOutput(true);
            return extractSlaveAddr(conn.getInputStream(), slaveId);
        } catch (IOException e) {
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


    private static Optional<String> fetchDirectory(String slave, String frameworkId, String executorId) {
        try {
            URL url = new URL("http://" + slave + "/state");
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");
            conn.setDoOutput(true);
            return extractDirectory(conn.getInputStream(), frameworkId, executorId);

        } catch (MalformedURLException e) {
            // REVIEW: catch(MalformedURLException) clause can be removed because it <: IOException
            return Optional.empty();
        } catch (IOException e) {
            return Optional.empty();
        }
    }

    public static Optional<String> extractDirectory(InputStream stream, String frameworkId, String executorId) throws IOException {
        // REVIEW: this may prepare corresponded object type instead of using java.util.Map
        //  { ... "frameworks" : [ { ... "executors":[ { "id":"sum", "completed_tasks":[], "tasks":[], "queued_tasks":[]} ] ...
        ObjectMapper mapper = new ObjectMapper();
        Map<String, Map<String, Object>> map = mapper.readValue(stream, java.util.Map.class);
        List<Map<String, Object>> list = (List) map.get("frameworks");

        // TODO: use json-path for cleaner and flexible code
        for (Map<String, Object> executors : list) {
            if (executors.get("executors") != null) {
                for (Map<String, Object> executor : (List<Map<String, Object>>) executors.get("executors")) {
                    if (executor.get("id").equals(executorId)) {
                        return Optional.ofNullable((String) executor.get("directory"));
                    }
                }
            }
        }
        return Optional.empty();
    }

    public static Optional<String> extractContainerId(InputStream stream, String frameworkId, String executorId) throws IOException {
        //  { ... "frameworks" : [ { ... "executors":[ { "id":"sum", "completed_tasks":[], "tasks":[], "queued_tasks":[]} ] ...
        ObjectMapper mapper = new ObjectMapper();
        Map<String, Map<String, Object>> map = mapper.readValue(stream, java.util.Map.class);
        List<Map<String, Object>> list = (List) map.get("frameworks");

        // TODO: use json-path for cleaner and flexible code
        for (Map<String, Object> executors : list) {
            if (executors.get("executors") != null) {
                for (Map<String, Object> executor : (List<Map<String, Object>>) executors.get("executors")) {
                    if (executor.get("id").equals(executorId)) {
                        return Optional.ofNullable((String) executor.get("container"));
                    }
                }
            }
        }
        return Optional.empty();
    }

    public static List<Map<String, Object>> fetchTasks(String master, String frameworkId, int offset, int limit) throws MalformedURLException {
        URL url = new URL("http://" + master + "/tasks?offset=" + offset + "&limit=" + limit);
        HttpURLConnection conn;
        try {
            conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");
            conn.setDoOutput(true);
            return parseTasks(conn.getInputStream(), frameworkId);
        } catch (IOException e) {
            return new LinkedList<>();
        }
    }

    public static List<Map<String, Object>> parseTasks(InputStream in, String frameworkId) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        List<Map<String, Object>> ret = new LinkedList<>();

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

    public static void downloadHTTPFile(String url, String name, Receivable<Triad<Integer, String, Pair<Long, InputStream>>, IOException> cb) throws IOException {
        String addr = url.replace("files/browse", "files/download") + "%2F" + maybeURLEncode(name);
        LOG.debug("Downloading {}", addr);

        HttpURLConnection conn = null;
        try {
            conn = (HttpURLConnection) new URL(addr).openConnection();
            conn.setRequestMethod("GET");

            Integer statusCode = conn.getResponseCode();
            String message = conn.getResponseMessage();
            Long length = conn.getHeaderFieldLong("Content-Length", -1);
            LOG.debug("res={}, md5={}, length={}", message,
                    conn.getHeaderField("Content-md5"), length);

            cb.receive(new Triad<>(statusCode, message, new Pair<>(length, conn.getInputStream())));
        }
        finally {
            if (conn != null) {
                conn.disconnect();
            }
        }

    }


    // Actually Mesos 1.1.0 returns whole body even if it gets HEAD request.
    public static boolean statHTTPFile(String url, String name) {
        String addr = url.replace("files/browse", "files/download") + "%2F" + maybeURLEncode(name);

        HttpURLConnection conn = null;
        try {
            conn = (HttpURLConnection) new URL(addr).openConnection();
            conn.setRequestMethod("HEAD");
            conn.setDoOutput(false);
            LOG.debug(conn.getResponseMessage());
            return conn.getResponseCode() == 200 ||
                    conn.getResponseCode() == 204;
        } catch (IOException e) {
            LOG.debug("Failed to fetch {}: {}", addr, e.toString());
            return false;
        } finally {
            if (conn != null) {
                conn.disconnect();
            }
        }
    }

    private static Pair<Integer, String> fetchHTTP(String addr) throws IOException {
        return fetchHTTP(addr, 3);
    }

    // Only for String contents
    private static Pair<Integer, String> fetchHTTP(String addr, int retry) throws IOException {
        LOG.debug("Fetching {}", addr);
        HttpURLConnection conn = null;

        conn = (HttpURLConnection) new URL(addr).openConnection();
        conn.setRequestMethod("GET");
        conn.setDoOutput(true);
        LOG.debug("{} {} for {}", conn.getResponseCode(), conn.getResponseMessage(), addr);

        if (conn.getResponseCode() != 200) {
            if (conn.getResponseCode() < 200) {
                return fetchHTTP(addr, retry - 1);
            } else if (conn.getResponseCode() < 300) {
                return new Pair<>(conn.getResponseCode(), ""); // Mostly 204; success
            } else if (conn.getResponseCode() < 400) {
                // TODO: Mesos master failover
                return new Pair<>(conn.getResponseCode(), conn.getResponseMessage());
            } else if (conn.getResponseCode() == 404) {
                throw new FileNotFoundException(addr);
            } else {
                return new Pair<>(conn.getResponseCode(), conn.getResponseMessage());
            }
        }

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream(), UTF_8))) {
            StringBuilder builder = new StringBuilder();
            String line;
            do {
                line = reader.readLine();
                builder.append(line);
            } while (line != null);
            LOG.debug("Fetched {} bytes from {}", builder.toString().length(), addr);
            return new Pair<>(conn.getResponseCode(), builder.toString());

        } catch (FileNotFoundException e) {
            throw e;
        } catch (IOException e) {
            // Somehow this happens even HTTP was correct
            LOG.debug("Cannot fetch file {}: {}", addr, e.toString());
            // Just retry until your stack get stuck; thanks to SO:33340848
            // and to that crappy HttpURLConnection
            if (retry < 0) {
                LOG.error("Retry failed. Last error was: {}", e.toString());
                throw e;
            }
            return fetchHTTP(addr, retry - 1);
        } finally {
            conn.disconnect();
        }
    }

    public static Pair<Integer, String> fetchHTTPFile(String url, String name, long offset, long length) throws MalformedURLException, IOException {
        String addr = url.replace("files/browse", "files/read") + "%2F" + maybeURLEncode(name)
                + "&offset=" + offset + "&length=" + length;
        return fetchHTTP(addr);
    }

    public static Pair<Integer, String> fetchHTTPDir(String url, String path) throws MalformedURLException, IOException {
        // Just do 'files/browse and get JSON
        String addr = url + "%2F" + maybeURLEncode(path);
        return fetchHTTP(addr);
    }

    private static String maybeURLEncode(String file) {
        try {
            return URLEncoder.encode(file, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            return file;
        }
    }
}