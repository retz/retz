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
package io.github.retz.jmx;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import io.github.retz.bean.AdminConsoleMXBean;
import io.github.retz.db.Database;
import io.github.retz.misc.LogUtil;
import io.github.retz.protocol.data.Job;
import io.github.retz.protocol.data.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class AdminConsole implements AdminConsoleMXBean {
    private static final Logger LOG = LoggerFactory.getLogger(AdminConsole.class);

    private final ObjectMapper mapper = new ObjectMapper();
    private final int defaultLeeway;

    public AdminConsole(int leeway) {
        mapper.registerModule(new Jdk8Module());
        this.defaultLeeway = leeway;
    }

    @Override
    public String createUser(String info) throws IOException {
        LOG.info("AdminConsole.createUser({})", info);
        User user = Database.getInstance().createUser(info);
        LOG.info(maybeEncodeAsJSON(user));
        return maybeEncodeAsJSON(user);
    }

    @Override
    public String getUser(String name) throws IOException {
        LOG.info("AdminConsole.getUser({})", name);
        Optional<User> maybeUser = Database.getInstance().getUser(name);
        return maybeEncodeAsJSON(maybeUser);
    }

    @Override
    public boolean enableUser(String id, boolean enabled) throws IOException {
        LOG.info("AdminConsole.enableUser({}, {})", id, enabled);
        Database.getInstance().enableUser(id, enabled);
        return true;
    }

    @Override
    public List<String> getUsage(String start, String end) throws IOException {
        LOG.info("Querying usage at [{}, {})", start, end); //TODO
        List<Job> jobs = Database.getInstance().finishedJobs(start, end);
        return jobs.stream().map(job -> maybeEncodeAsJSON(job)).collect(Collectors.toList());
    }

    @Override
    public List<String> listUser() throws IOException {
        LOG.info("AdminConsole.listUser()");

        List<User> users = Database.getInstance().allUsers();
        return users.stream().map(user -> user.keyId()).collect(Collectors.toList());

    }

    @Override
    public boolean gc() {
        return gc(defaultLeeway);
    }

    @Override
    public boolean gc(int leeway) {
        try {
            // TODO: do we do mutex to avoid concurrent execution even though it has transaction?
            LOG.info("Job GC invocation from JMX: leeway={}s", leeway);
            Database.getInstance().deleteOldJobs(leeway);
            return true;
        } catch (Throwable t) {
            LogUtil.info(LOG, "AdminConsole.gc() failed", t);
            return false;
        }
    }

    private String maybeEncodeAsJSON(Object o) {
        try {
            return mapper.writeValueAsString(o);
        } catch (JsonProcessingException e) {
            LogUtil.error(LOG, "AdminConsole.maybeEncodeAsJSON() failed", e);
            return errorJSON(e.toString());
        }
    }

    private String errorJSON(String message) {
        return "{\"error\":\"" + message + "\"}";
    }
}
