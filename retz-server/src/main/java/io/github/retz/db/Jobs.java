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
package io.github.retz.db;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.retz.cli.TimestampHelper;
import io.github.retz.protocol.data.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class Jobs {
    private static final Logger LOG = LoggerFactory.getLogger(Jobs.class);

    private Connection conn;
    private ObjectMapper mapper;

    public Jobs(Connection c, ObjectMapper m) throws SQLException {
        this.conn = Objects.requireNonNull(c);
        this.mapper = Objects.requireNonNull(m);
        if (conn.getAutoCommit()) {
            throw new RuntimeException("Connection must have autocommit disabled");
        }
    }

    public List<Job> getAllRunning() throws SQLException {
        List<Job> ret = new ArrayList<>();
        try (PreparedStatement p = conn.prepareStatement("SELECT json FROM jobs WHERE state='STARTING' OR state='STARTED'");
             ResultSet res = p.executeQuery()) {
            while (res.next()) {
                String json = res.getString("json");
                ret.add(mapper.readValue(json, Job.class));
            }
            for (Job job : ret) {
                job.doRetry();
                LOG.info("Retrying job: {}", job);
                updateJob(job);
            }
        } catch (IOException e) {
            throw new RuntimeException("Broken JSON: " + e.toString());
        }
        return ret;
    }

    public void doRetry(List<Integer> ids) {
        try {
            for (int id : ids) {
                Optional<Job> maybeJob = getJob(id);
                if (maybeJob.isPresent()) {
                    Job job = maybeJob.get();
                    job.doRetry();
                    updateJob(job);
                }
            }
        } catch (SQLException e) {
            LOG.error(e.toString(), e);
        } catch (IOException e) {
            LOG.error(e.toString(), e); // TODO: do we fail?
        }
    }

    public Optional<Job> getJob(int id) throws SQLException, IOException {
        try (PreparedStatement p = conn.prepareStatement("SELECT json FROM jobs WHERE id=?")) {
            p.setInt(1, id);
            try (ResultSet res = p.executeQuery()) {
                if (res.next()) {
                    String json = res.getString("json");
                    return Optional.ofNullable(mapper.readValue(json, Job.class));
                }
            }
        }
        return Optional.empty();
    }

    public void updateJob(Job j) throws SQLException, JsonProcessingException {
        LOG.debug("Updating job as name={}, id={}, appid={}", j.name(), j.id(), j.appid());
        try (PreparedStatement p = conn.prepareStatement("UPDATE jobs SET name=?, appid=?, cmd=?, priority=?, taskid=?, state=?, started=?, finished=?, json=? WHERE id=?")) {
            p.setString(1, j.name());
            p.setString(2, j.appid());
            p.setString(3, j.cmd());
            p.setInt(4, j.priority());
            p.setString(5, j.taskId());
            p.setString(6, j.state().toString());
            p.setString(7, j.started());
            p.setString(8, j.finished());
            p.setString(9, mapper.writeValueAsString(j));
            p.setInt(10, j.id());
            p.execute();
        }
    }

    public void collect(int leeway) throws SQLException {
        String last = TimestampHelper.past(leeway);
        try (PreparedStatement p = conn.prepareStatement("DELETE FROM jobs WHERE finished < ? AND (state='FINISHED' OR state='KILLED')"))
        {
            LOG.info("Deleting old jobs finished before {}...", last);
            p.setString(1, last);
            p.execute(); // returns true as the result of DELETE query is null.
        }
    }
}
