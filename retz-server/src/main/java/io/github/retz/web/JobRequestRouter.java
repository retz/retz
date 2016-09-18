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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import io.github.retz.protocol.*;
import io.github.retz.protocol.data.DirEntry;
import io.github.retz.protocol.data.FileContent;
import io.github.retz.protocol.data.Job;
import io.github.retz.scheduler.JobQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static java.nio.charset.StandardCharsets.UTF_8;

public class JobRequestRouter {
    private static final Logger LOG = LoggerFactory.getLogger(JobRequestRouter.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    static {
        MAPPER.registerModule(new Jdk8Module());
    }

    public static String getJob(spark.Request req, spark.Response res) throws JsonProcessingException, IOException {
        int id = Integer.parseInt(req.params(":id"));

        LOG.debug("get job id={}, path={}, file={}", id);
        res.type("application/json");

        Optional<Job> job = JobQueue.getJob(id);

        Response response;
        // Search job from JobQueue with matching id
        GetJobResponse getJobResponse = new GetJobResponse(job);
        getJobResponse.ok();
        res.status(200);

        response = getJobResponse;

        return MAPPER.writeValueAsString(response);

    }

    public static String getFile(spark.Request req, spark.Response res) throws IOException {
        int id = Integer.parseInt(req.params(":id"));

        String file = req.params("file");
        int offset = Integer.parseInt(req.queryParams("offset"));
        int length = Integer.parseInt(req.queryParams("length"));
        Optional<Job> job = JobQueue.getJob(id);

        LOG.info("get-file: id={}, path={}, offset={}, length={}", id, file, offset, length);
        res.type("application/json");

        Optional<FileContent> fileContent;
        if (job.isPresent() && job.get().url() != null // If url() is null, the job hasn't yet been started at Mesos
                && statHTTPFile(job.get().url(), file)) {
            String payload = fetchHTTPFile(job.get().url(), file, offset, length);
            LOG.debug("Payload length={}, offset={}", payload.length(), offset);
            // TODO: what the heck happens when a file is not UTF-8 encodable???? How Mesos works?
            fileContent = Optional.ofNullable(MAPPER.readValue(payload, FileContent.class));
        } else {
            fileContent = Optional.empty();
        }
        GetFileResponse getFileResponse = new GetFileResponse(job, fileContent);
        getFileResponse.ok();
        res.status(200);

        return MAPPER.writeValueAsString(getFileResponse);

    }

    public static String getPath(spark.Request req, spark.Response res) throws IOException {
        int id = Integer.parseInt(req.params(":id"));

        String path = req.params("path");
        Optional<Job> job = JobQueue.getJob(id);

        LOG.debug("get-path: id={}, path={}", id, path);
        res.type("application/json");

        // Translating default as SparkJava's router doesn't route '.' or empty string
        if (ListFilesRequest.DEFAULT_SANDBOX_PATH.equals(path)) {
            path = "";
        }

        List ret;
        if (job.isPresent() && job.get().url() != null) {
            try {
                String json = fetchHTTPDir(job.get().url(), path);
                ret = MAPPER.readValue(json, new TypeReference<List<DirEntry>>() {
                });
            } catch (FileNotFoundException e) {
                res.status(404);
                LOG.warn("path {} not found", path);
                return MAPPER.writeValueAsString(new ErrorResponse(path + " not found"));
            }
        } else {
            ret = Arrays.asList();
        }

        ListFilesResponse listFilesResponse = new ListFilesResponse(job, ret);

        return MAPPER.writeValueAsString(listFilesResponse);
    }


    public static boolean statHTTPFile(String url, String name) {
        String addr = url.replace("files/browse", "files/download") + "%2F" + name;

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

    private static String fetchHTTP(String addr) throws MalformedURLException, IOException {
        return fetchHTTP(addr, 3);
    }

    private static String fetchHTTP(String addr, int retry) throws MalformedURLException, IOException {
        LOG.info("Fetching {}", addr);
        HttpURLConnection conn = null;
        try {
            conn = (HttpURLConnection) new URL(addr).openConnection();
            conn.setRequestMethod("GET");
            conn.setDoOutput(true);
            LOG.debug(conn.getResponseMessage());

        } catch (MalformedURLException e) {
            LOG.error(e.toString());
            throw e;
        } catch (IOException e) {
            LOG.error(e.toString());
            throw e;
        }

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream(), UTF_8))) {
            StringBuilder builder = new StringBuilder();
            String line;
            do {
                line = reader.readLine();
                builder.append(line);
            } while (line != null);
            LOG.debug("Fetched {} bytes from {}", builder.toString().length(), addr);
            return builder.toString();

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

    public static String fetchHTTPFile(String url, String name, int offset, int length) throws MalformedURLException, IOException {
        String addr = url.replace("files/browse", "files/read") + "%2F" + name
                + "&offset=" + offset + "&length=" + length;
        return fetchHTTP(addr);
    }

    public static String fetchHTTPDir(String url, String path) throws MalformedURLException, IOException {
        // Just do 'files/browse and get JSON
        String addr = url + "%2F" + path;
        return fetchHTTP(addr);
    }
}
