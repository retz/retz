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
package io.github.retz.cli;

import com.beust.jcommander.Parameter;
import io.github.retz.protocol.ErrorResponse;
import io.github.retz.protocol.GetFileResponse;
import io.github.retz.protocol.Response;
import io.github.retz.protocol.data.Job;
import io.github.retz.web.Client;
import io.github.retz.web.ClientHelper;
import org.apache.commons.io.FilenameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.Calendar;
import java.util.Date;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeoutException;

import static java.nio.charset.StandardCharsets.UTF_8;

public class CommandGetFile implements SubCommand {
    static final Logger LOG = LoggerFactory.getLogger(CommandGetFile.class);

    @Parameter(names = {"-i", "--id", "-id"}, description = "Job ID whose state and details you want", required = true)
    private int id;

    @Parameter(names = {"-R", "--resultdir"}, description = "Local directory to save the file ('-' to print)")
    private String resultDir = "-";

    @Parameter(names = "--path", description = "Remote file path to fetch")
    private String filename = "stdout";

    @Parameter(names = "--poll", description = "Keep polling the file until a job finishes")
    private boolean poll = false;

    @Parameter(names = "--offset", description = "Offset")
    private long offset = 0;

    @Parameter(names = "--length", description = "Length")
    private long length = -1; // -1 means get all file

    @Parameter(names = "--binary", description = "Whether the file is binary or not. This option must be combined with '-R'.")
    private boolean isBinary = false;

    @Parameter(names = "--timeout", description = "Timeout in minutes until kill from client (-1 or 0 for no timeout, default is 24 hours)")
    int timeout = 24 * 60;

    @Override
    public String description() {
        return "Get file from sandbox of a job";
    }

    @Override
    public String getName() {
        return "get-file";
    }

    @Override
    public int handle(ClientCLIConfig fileConfig, boolean verbose) throws Throwable {
        LOG.debug("Configuration: {}", fileConfig.toString());

        try (Client webClient = Client.newBuilder(fileConfig.getUri())
                .setAuthenticator(fileConfig.getAuthenticator())
                .checkCert(!fileConfig.insecure())
                .setVerboseLog(verbose)
                .build()) {

            if (verbose) {
                LOG.info("Getting file {} (offset={}, length={}) of a job(id={})", filename, offset, length, id);
            }

            if (isBinary) {
                if ("-".equals(resultDir)) {
                    LOG.error("--binary must be with -R option, to download the file.");
                    return -1;
                } else if (poll) {
                    LOG.error("--binary cannot work with --poll");
                    return -1;
                }


                LOG.info("Binary mode: ignoring offset and length but downloading while file to '{}/{}'.", resultDir, filename);
                ClientHelper.getWholeBinaryFile(webClient, id, filename, resultDir);
                return 0;
            }


            Date start = Calendar.getInstance().getTime();
            Callable<Boolean> timedout;
            if (timeout > 0) {
                timedout = () -> {
                    Date now = Calendar.getInstance().getTime();
                    long diff = now.getTime() - start.getTime();
                    return diff / 60 > timeout * 60;
                };
            } else {
                timedout = () -> false;
            }

            OutputStream out = this.tentativeOutputStream(webClient, resultDir, filename);
            if (length < 0) {
                try {
                    ClientHelper.getWholeFileWithTerminator(webClient, id, filename, poll, out, timedout);
                } catch (TimeoutException e) {
                    webClient.kill(id);
                    LOG.error("Job(id={}) has been killed due to timeout after {} minute(s)", id, timeout);
                    return -1;
                }
                return 0;
            }

            Response res = webClient.getFile(id, filename, offset, length);

            if (res instanceof GetFileResponse) {
                GetFileResponse getFileResponse = (GetFileResponse) res;

                if (getFileResponse.job().isPresent()) {
                    Job job = getFileResponse.job().get();

                    if (getFileResponse.file().isPresent()) {
                        if (verbose) {
                            LOG.info("offset={}", getFileResponse.file().get().offset());
                        }
                        out.write(getFileResponse.file().get().data().getBytes(UTF_8));

                    } else if (verbose) {
                        LOG.info("Job: {}", job);
                    }

                    if (out != null && "-".equals(resultDir)) {
                        out.close();
                    }
                    return 0;

                } else {
                    LOG.error("No such job: id={}", id);
                }
            } else {
                ErrorResponse errorResponse = (ErrorResponse) res;
                LOG.error("Error: {}", errorResponse.status());
            }
        }
        return -1;
    }

    private OutputStream tentativeOutputStream(Client c, String resultDir, String filename) throws FileNotFoundException {
        if ("-".equals(resultDir)) {
            return System.out;
        } else {
            String basename = FilenameUtils.getName(filename);
            String path = resultDir + "/" + basename;
            LOG.info("Saving {} to {}", filename, path);
            return new FileOutputStream(path);
        }
    }
}

