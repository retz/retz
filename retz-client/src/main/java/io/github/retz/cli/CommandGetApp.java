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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import io.github.retz.protocol.ErrorResponse;
import io.github.retz.protocol.GetAppResponse;
import io.github.retz.protocol.Response;
import io.github.retz.protocol.data.Application;
import io.github.retz.web.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommandGetApp implements SubCommand {
    static final Logger LOG = LoggerFactory.getLogger(CommandGetApp.class);

    @Parameter(names = {"-A", "--appname"}, required = true, description = "Application name you loaded")
    private String appName;

    @Override
    public String description() {
        return "Get an application details";
    }

    @Override
    public String getName() {
        return "get-app";
    }

    @Override
    public int handle(ClientCLIConfig fileConfig, boolean verbose) throws Throwable {
        LOG.debug("Configuration: {}", fileConfig.toString());

        try (Client webClient = Client.newBuilder(fileConfig.getUri())
                .setAuthenticator(fileConfig.getAuthenticator())
                .checkCert(!fileConfig.insecure())
                .setVerboseLog(verbose)
                .build()) {

            Response res = webClient.getApp(appName);
            if (res instanceof ErrorResponse) {
                LOG.error(res.status());
            } else if (res instanceof GetAppResponse) {
                GetAppResponse getAppResponse = (GetAppResponse) res;
                Application app = getAppResponse.application();

                ObjectMapper mapper = new ObjectMapper();
                mapper.registerModule(new Jdk8Module());
                mapper.writeValue(System.out, app);
                System.out.println();
                return 0;
            }
        }
        return -1;
    }
}
