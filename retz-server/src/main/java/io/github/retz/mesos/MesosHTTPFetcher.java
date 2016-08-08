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
package io.github.retz.mesos;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Optional;

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
        LOG.info("{}", slaveAddr);

        if (!slaveAddr.isPresent()) {
            return Optional.empty();
        }

        Optional<String> dir = fetchDirectory(slaveAddr.get(), frameworkId, executorId);
        if (!dir.isPresent()) {
            return Optional.empty();
        }
        LOG.info("{}", dir);


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
}
