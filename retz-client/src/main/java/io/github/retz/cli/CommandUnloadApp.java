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

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import io.github.retz.protocol.LoadAppResponse;
import io.github.retz.protocol.UnloadAppResponse;
import io.github.retz.web.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.ConnectException;
import java.net.URISyntaxException;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

public class CommandUnloadApp implements SubCommand {
    static final Logger LOG = LoggerFactory.getLogger(CommandUnloadApp.class);

    @Parameter(names = {"-A", "--appname"}, required = true, description = "Application name you loaded")
    private String appName;


    @Override
    public String description() {
        return "Unload app and release everything";
    }

    @Override
    public String getName() {
        return "unload-app";
    }

    @Override
    public int handle(FileConfiguration fileConfig) {
        LOG.debug("Configuration: {}", fileConfig.toString());


        try (Client webClient = new Client(fileConfig.getUri().getHost(),
                fileConfig.getUri().getPort())) {
            webClient.connect();

            UnloadAppResponse res = (UnloadAppResponse) webClient.unload(appName);
            LOG.info(res.status());
            return 0;

        } catch (URISyntaxException e) {
            LOG.error(e.getMessage());
        } catch (ConnectException e) {
            LOG.error("Cannot connect to server {}", fileConfig.getUri());
        } catch (ExecutionException e) {
            LOG.error(e.getMessage());
        } catch (IOException e) {
            e.printStackTrace();
        }
        return -1;
    }
}
