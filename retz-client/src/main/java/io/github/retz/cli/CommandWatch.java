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

import io.github.retz.web.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.ConnectException;
import java.net.URISyntaxException;

public class CommandWatch implements SubCommand{
    static final Logger LOG = LoggerFactory.getLogger(CommandWatch.class);

    @Override
    public String description() {
        return "Watch the whole job queue changes";
    }

    @Override
    public String getName() {
        return "watch";
    }

    @Override
    public int handle(FileConfiguration fileConfig) {
        LOG.debug("Configuration: {}", fileConfig.toString());

        try (Client webClient = new Client(fileConfig.getUri().getHost(),
                fileConfig.getUri().getPort())) {
           // webClient.connect();

            webClient.startWatch((watchResponse -> {
                StringBuilder b = new StringBuilder()
                        .append("event: ").append(watchResponse.event());

                if (watchResponse.job() != null) {
                    b.append(" Job ").append(watchResponse.job().id())
                            .append(" (app=").append(watchResponse.job().appid())
                            .append(") has ").append(watchResponse.event())
                            .append(" at ");
                    if (watchResponse.event().equals("started")) {
                        b.append(watchResponse.job().started());
                        b.append(" cmd=").append(watchResponse.job().cmd());

                    } else if (watchResponse.event().equals("scheduled")) {
                        b.append(watchResponse.job().scheduled());
                        b.append(" cmd=").append(watchResponse.job().cmd());

                    } else if (watchResponse.event().equals("finished")) {
                        b.append(watchResponse.job().finished());
                        b.append(" result=").append(watchResponse.job().result());
                        b.append(" url=").append(watchResponse.job().url());
                    } else {
                        b.append("unknown event(error)");
                    }
                }
                LOG.info(b.toString());
                return true;
            }));
            return 0;

        } catch (ConnectException e) {
            LOG.error("Cannot connect to server {}", fileConfig.getUri());
        } catch (IOException e) {
            LOG.error(e.toString(), e);
        }
        return -1;
    }
}
