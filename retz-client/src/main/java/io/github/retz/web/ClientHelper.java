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
package io.github.retz.web;

import io.github.retz.protocol.*;
import io.github.retz.protocol.data.DirEntry;
import io.github.retz.protocol.data.Job;
import io.github.retz.protocol.exception.JobNotFoundException;
import org.apache.commons.io.FilenameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeoutException;

import static java.nio.charset.StandardCharsets.UTF_8;

public final class ClientHelper {
    static final Logger LOG = LoggerFactory.getLogger(ClientHelper.class);
    static final int MAX_INTERVAL_MSEC = 32768;
    static final int INITAL_INTERVAL_MSEC = 512;

    private ClientHelper() {
        throw new UnsupportedOperationException();
    }

    public static List<Job> queue(Client c) throws IOException {
        Response res = c.list(Job.JobState.QUEUED, Optional.empty());
        return ((ListJobResponse) res).jobs();
    }

    public static List<Job> running(Client c) throws IOException {
        List<Job> jobs = new ArrayList<>();
        Response res = c.list(Job.JobState.STARTING, Optional.empty());
        jobs.addAll(((ListJobResponse) res).jobs());
        res = c.list(Job.JobState.STARTED, Optional.empty());
        jobs.addAll(((ListJobResponse) res).jobs());
        return jobs;
    }

    public static List<Job> finished(Client c) throws IOException {
        List<Job> jobs = new ArrayList<>();
        Response res = c.list(Job.JobState.FINISHED, Optional.empty());
        jobs.addAll(((ListJobResponse) res).jobs());
        res = c.list(Job.JobState.KILLED, Optional.empty());
        jobs.addAll(((ListJobResponse) res).jobs());
        return jobs;
    }


    public static boolean fileExists(Client c, long id, String filename) throws IOException {
        String directory = new File(filename).getParent();
        if (directory == null) {
            directory = ListFilesRequest.DEFAULT_SANDBOX_PATH;
        }
        Response response = c.listFiles(id, directory);
        if (response instanceof ListFilesResponse) {
            ListFilesResponse listFilesResponse = (ListFilesResponse) response;
            for (DirEntry e : listFilesResponse.entries()) {
                if (e.path().endsWith(filename)) {
                    return true;
                }
            }
        } else {
            LOG.warn(response.status());
        }
        return false;
    }

    public static void getWholeFile(Client c, int id, String filename, String resultDir)
            throws IOException {
        try {
            getWholeFileWithTerminator(c, id, filename, resultDir, null);
        } catch (TimeoutException e) {
            LOG.error(e.toString());
        }
    }

    public static void getWholeFileWithTerminator(Client c, int id, String filename, String resultDir, Callable<Boolean> terminator)
            throws IOException, TimeoutException {
        String path = resultDir + "/" + filename;
        try (FileOutputStream out = new FileOutputStream(path)) {
            getWholeFileWithTerminator(c, id, filename, false, out, terminator);

        } catch (FileNotFoundException e) {
            LOG.error(e.toString());
        } catch (JobNotFoundException e) {
            LOG.error(e.toString());
        }
    }

    // Gets whole file until the job finishes and streams out to 'out'!!!
    // Throws FileNotFoundException when no file found, unlike getFile
    public static Optional<Job> getWholeFile(Client c, int id, String filename, boolean poll, OutputStream out)
            throws JobNotFoundException, IOException {
        try {
            return getWholeFileWithTerminator(c, id, filename, poll, out, null);
        } catch (TimeoutException e) {
            LOG.error(e.toString());
            return Optional.empty();
        }
    }

    public static Optional<Job> getWholeFileWithTerminator(Client c, long id, String filename, boolean poll, OutputStream out, Callable<Boolean> terminate)
            throws IOException, JobNotFoundException, TimeoutException {
        return getWholeFileWithTerminator(c, id, filename, poll, out, 0, terminate);
    }

    public static Optional<Job> getWholeFileWithTerminator(Client c, long id, String filename, boolean poll, OutputStream out, long offset, Callable<Boolean> terminator)
            throws IOException, JobNotFoundException, TimeoutException {
        Optional<Job> current;

        {
            Response res = c.getJob(id);
            if (!(res instanceof GetJobResponse)) {
                LOG.error(res.status());
                throw new IOException(res.status());
            }
            GetJobResponse getJobResponse = (GetJobResponse) res;
            if (!getJobResponse.job().isPresent()) {
                throw new JobNotFoundException(id);
            }
        }

        int interval = INITAL_INTERVAL_MSEC;
        Job.JobState currentState = Job.JobState.QUEUED;

        long bytesRead = readFileUntilEmpty(c, id, filename, offset, out);
        offset = offset + bytesRead;

        do {
            Response res = c.getJob(id);
            if (!(res instanceof GetJobResponse)) {
                LOG.error(res.status());
                throw new IOException(res.status());
            }
            GetJobResponse getJobResponse = (GetJobResponse) res;
            current = getJobResponse.job();

            bytesRead = readFileUntilEmpty(c, id, filename, offset, out);
            offset = offset + bytesRead;

            if (current.isPresent()) {
                currentState = current.get().state();
                if ((currentState == Job.JobState.FINISHED || currentState == Job.JobState.KILLED)
                        && bytesRead == 0) {
                    break;
                }
            }

            if (poll) {
                maybeSleep(interval);

                if (bytesRead == 0) {
                    interval = Math.min(interval * 2, MAX_INTERVAL_MSEC);
                } else {
                    interval = INITAL_INTERVAL_MSEC;
                }

                try {
                    if (terminator != null && terminator.call()) {
                        throw new TimeoutException("Timeout at getWholeFile");
                    }
                } catch (TimeoutException e) {
                    throw e;
                } catch (Exception e) {
                    LOG.error(e.toString(), e);
                    return current; // I don't know how to handle it
                }
            } else {
                break;
            }
        }
        while (currentState != Job.JobState.FINISHED && currentState != Job.JobState.KILLED);

        if (!ClientHelper.fileExists(c, id, filename)) {
            // TODO: remove a file if it's already created
            throw new FileNotFoundException(filename);
        }

        return current;
    }

    // This interface is still in experiment, which may be changed even in patch release.
    public static void getWholeBinaryFile(Client c, long id, String path, String output) throws IOException {
        String fullpath = FilenameUtils.concat(output, FilenameUtils.getName(path));
        LOG.info("Saving {} as {}", path, fullpath);
        try (FileOutputStream out = new FileOutputStream(fullpath)) {
            c.getBinaryFile(id, path, out);
        }
    }

    public static void getWholeBinaryFile(Client c, long id, String path, OutputStream out) throws IOException {
        c.getBinaryFile(id, path, out);
    }

    static long readFileUntilEmpty(Client c, long id, String filename, long offset, OutputStream out) throws IOException {
        int length = 65536;
        long current = offset;

        while (true) {
            Response res = c.getFile(id, filename, current, length);
            if (res instanceof GetFileResponse) {
                GetFileResponse getFileResponse = (GetFileResponse) res;

                // Check data
                if (getFileResponse.file().isPresent()) {
                    if (getFileResponse.file().get().data().isEmpty()) {
                        // All contents fetched
                        return current - offset;

                    } else {
                        byte[] data = getFileResponse.file().get().data().getBytes(UTF_8);
                        LOG.debug("Fetched data length={}, current={}", data.length, current);
                        out.write(data);
                        current = current + data.length;
                    }
                } else {
                    //LOG.info("{}: ,{}", filename, current);
                    return current - offset;
                }
            } else {
                LOG.error(res.status());
                throw new IOException(res.status());
            }
        }
    }

    public static Job waitForStart(Job job, Client c, Callable<Boolean> terminate) throws IOException, TimeoutException {
        Job current = job;
        int interval = INITAL_INTERVAL_MSEC;
        while (current.state() == Job.JobState.QUEUED) {
            maybeSleep(interval);
            interval = Math.min(interval * 2, MAX_INTERVAL_MSEC);

            try {
                if (terminate != null && terminate.call()) {
                    throw new TimeoutException("Timeout at waitForStart");
                }
            } catch (TimeoutException e) {
                throw e;
            } catch (Exception e) {
                LOG.error(e.toString(), e);
                return null; // I don't know how to handle it
            }

            Response res = c.getJob(job.id());
            if (res instanceof GetJobResponse) {
                GetJobResponse getJobResponse = (GetJobResponse) res;
                if (getJobResponse.job().isPresent()) {
                    current = getJobResponse.job().get();
                    continue;
                }
            } else {
                LOG.error(res.status());
                throw new IOException(res.status());
            }
        }
        return current;
    }

    public static void maybeSleep(int millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
        }
    }
}
