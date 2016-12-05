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
package io.github.retz.scheduler;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import io.github.retz.bean.AdminConsoleMXBean;
import io.github.retz.db.Database;
import io.github.retz.protocol.data.Job;
import io.github.retz.protocol.data.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class AdminConsole implements AdminConsoleMXBean {
    private static final Logger LOG = LoggerFactory.getLogger(AdminConsole.class);

    private final ObjectMapper MAPPER = new ObjectMapper();
    private final int LEEWAY;

    public AdminConsole(int leeway) {
        MAPPER.registerModule(new Jdk8Module());
        this.LEEWAY = leeway;
    }

    @Override
    public String createUser() {
        LOG.info("AdminConsole.createUser()");
        try {
            User user = Database.getInstance().createUser();
            return maybeEncodeAsJSON(user);
        } catch (SQLException e) {
            return errorJSON(e.toString());
        }
    }

    @Override
    public String getUser(String name) {
        LOG.info("AdminConsole.getUser({})", name);
        Optional<User> maybeUser = Database.getInstance().getUser(name);
        return maybeEncodeAsJSON(maybeUser);
    }

    @Override
    public boolean enableUser(String id, boolean enabled) {
        LOG.info("AdminConsole.enableUser({}, {})", id, enabled);
        Database.getInstance().enableUser(id, enabled);
        return false;
    }

    @Override
    public List<String> getUsage(String user, String start, String end) {
        LOG.info("Querying usage of {} at [{}, {})", user, start, end); //TODO
        List<Job> jobs = Database.getInstance().finishedJobs(user, start, end);
        return jobs.stream().map(job -> maybeEncodeAsJSON(job)).collect(Collectors.toList());
    }

    @Override
    public List<String> listUser() {
        LOG.info("AdminConsole.listUser()");
        List<User> users = Database.getInstance().allUsers();
        return users.stream().map(user -> user.keyId()).collect(Collectors.toList());
    }

    @Override
    public boolean gc() {
        return gc(LEEWAY);
    }

    @Override
    public boolean gc(int leeway) {
        try {
            // TODO: do we do mutex to avoid concurrent execution even though it has transaction?
            LOG.info("Job GC invocation from JMX: leeway={}s", leeway);
            Database.getInstance().deleteOldJobs(LEEWAY);
            return true;
        } catch (Throwable t) {
            LOG.info(t.toString(), t);
            return false;
        }
    }

    private String maybeEncodeAsJSON(Object o) {
        try {
            return MAPPER.writeValueAsString(o);
        } catch (IOException e) {
            LOG.error(e.toString());
            return errorJSON(e.toString());
        }
    }

    private String errorJSON(String message) {
        return "{\"error\":\"" + message + "\"}";
    }
}
