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
package io.github.retz.cli;

import com.beust.jcommander.Parameter;
import io.github.retz.protocol.ErrorResponse;
import io.github.retz.protocol.GetFileResponse;
import io.github.retz.protocol.Response;
import io.github.retz.protocol.data.Job;
import io.github.retz.web.Client;
import io.github.retz.web.ClientHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.ConnectException;

import static java.nio.charset.StandardCharsets.UTF_8;

public class CommandGetFile implements SubCommand {
    static final Logger LOG = LoggerFactory.getLogger(CommandGetFile.class);

    @Parameter(names = "-id", description = "Job ID whose state and details you want", required = true)
    private int id;

    @Parameter(names = {"-R", "--resultdir"}, description = "Local directory to save the file ('-' to print)")
    private String resultDir = "-";

    @Parameter(names = "--fetch", description = "Remote file to fetch")
    private String filename;

    @Parameter(names = "--poll", description = "Keep polling the file until a job finishes")
    private boolean poll = false;

    @Parameter(names = "--offset", description = "Offset")
    private int offset = 0;

    @Parameter(names = "--length", description = "Length")
    private int length = -1; // -1 means get all file

    @Override
    public String description() {
        return "Get file from sandbox of a job";
    }

    @Override
    public String getName() {
        return "get-file";
    }

    @Override
    public int handle(FileConfiguration fileConfig) {
        LOG.debug("Configuration: {}", fileConfig.toString());

        OutputStream out = null;
        try (Client webClient = Client.newBuilder(fileConfig.getUri())
                .enableAuthentication(fileConfig.authenticationEnabled())
                .setAuthenticator(fileConfig.getAuthenticator())
                .checkCert(fileConfig.checkCert())
                .build()) {

            if (filename == null) {
                filename = ClientHelper.maybeGetStdout(id, webClient);
            }
            out = this.tentativeOutputStream(webClient, resultDir, filename);

            LOG.info("Getting file {} (offset={}, length={}) of a job(id={})", filename, offset, length, id);

            if (length < 0) {
                LOG.info("============== printing {} of job id={} ================", filename, id);
                ClientHelper.getWholeFile(webClient, id, filename, poll, out);

            } else {
                Response res = webClient.getFile(id, filename, offset, length);
                if (res instanceof GetFileResponse) {
                    GetFileResponse getFileResponse = (GetFileResponse) res;

                    if (getFileResponse.job().isPresent()) {
                        Job job = getFileResponse.job().get();

                        LOG.info("Job: appid={}, id={}, scheduled={}, cmd='{}'", job.appid(), job.id(), job.scheduled(), job.cmd());
                        LOG.info("\tstarted={}, finished={}, state={}, result={}", job.started(), job.finished(), job.state(), job.result());

                        if (getFileResponse.file().isPresent()) {
                            LOG.info("offset={}", getFileResponse.file().get().offset());
                            out.write(getFileResponse.file().get().data().getBytes(UTF_8));
                        }

                        if (out != null && "-".equals(resultDir)) {
                            out.close();
                        }
                        return 0;

                    } else {
                        LOG.error("No such job: id={}", id);
                        if (out != null && "-".equals(resultDir)) {
                            out.close();
                        }
                    }
                } else {
                    if (out != null && "-".equals(resultDir)) {
                        out.close();
                    }
                    ErrorResponse errorResponse = (ErrorResponse) res;
                    LOG.error("Error: {}", errorResponse.status());
                }
            }

        } catch (ConnectException e) {
            LOG.error("Cannot connect to server {}", fileConfig.getUri());
        } catch (IOException e) {
            LOG.error(e.toString(), e);

        } finally {
            if (out != null) {
                try {
                    out.close();
                } catch (Exception e) {
                }
            }
        }
        return -1;
    }

    private OutputStream tentativeOutputStream(Client c, String resultDir, String filename) throws FileNotFoundException {
        if ("-".equals(resultDir)) {
            return System.out;
        } else {
            String path = resultDir + "/" + filename;
            return new FileOutputStream(path);
        }
    }
}

