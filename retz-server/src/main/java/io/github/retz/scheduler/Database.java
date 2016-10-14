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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import io.github.retz.cli.FileConfiguration;
import io.github.retz.cli.TimestampHelper;
import io.github.retz.protocol.data.Application;
import io.github.retz.protocol.data.Job;
import io.github.retz.protocol.data.User;
import org.h2.jdbcx.JdbcConnectionPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.util.*;
import java.util.function.Function;

import static java.nio.charset.StandardCharsets.UTF_8;

// TODO: make this Singleton?
public class Database {
    private static final Logger LOG = LoggerFactory.getLogger(Database.class);
    private static String databaseURL = null;
    private static JdbcConnectionPool pool = null;

    private static final ObjectMapper MAPPER = new ObjectMapper();

    static {
        MAPPER.registerModule(new Jdk8Module());
    }

    private Database() {
    }

    public static void init(FileConfiguration config) throws IOException {
        databaseURL = Objects.requireNonNull(config.getDatabaseURL());
        LOG.info("Initializing database {}", databaseURL);

        pool = JdbcConnectionPool.create(config.getDatabaseURL(), "", "");

        try (Connection conn = pool.getConnection()) {
            conn.setAutoCommit(false);
            DatabaseMetaData meta = conn.getMetaData();

            if (!meta.supportsTransactionIsolationLevel(Connection.TRANSACTION_SERIALIZABLE)) {
                LOG.error("Current database ({}) does not support required isolation level ({})",
                        databaseURL, Connection.TRANSACTION_SERIALIZABLE);
                throw new RuntimeException("Current database does not support serializable");
            }
            maybeCreateTables(conn);
            conn.commit();
        } catch (SQLException e) {
            LOG.error(e.toString());
        }

        addUser(config.getUser());
        if (getUser(config.getAccessKey()).isPresent()) {
            LOG.info("admin user is {}", config.getAccessKey());
        } else {
            LOG.error("No admin user created in database");
            throw new RuntimeException("orz");
        }
    }

    public static void stop() {
        LOG.info("Stopping database {}", databaseURL);
        while (pool.getActiveConnections() > 0) {
            try {
                Thread.sleep(512);
            } catch (InterruptedException e) {
            }
        }
        pool.dispose();
    }

    static boolean allTableExists(Connection conn) throws SQLException {
        boolean userTableExists = false;
        boolean applicationTableExists = false;
        boolean jobTableExists = false;

        DatabaseMetaData meta = conn.getMetaData();

        ResultSet res = meta.getTables(null, "PUBLIC", null, null);

        while (res.next()) {
            String tableName = res.getString("TABLE_NAME");
            if ("USERS".equals(tableName)) {
                userTableExists = true;
            } else if ("APPLICATIONS".equals(tableName)) {
                applicationTableExists = true;
            } else if ("JOBS".equals(tableName)) {
                jobTableExists = true;
            }
            LOG.info("category={}, scheme={}, name={}, type={}, remarks={}",
                    res.getString("TABLE_CAT"), res.getString("TABLE_SCHEM"),
                    res.getString("TABLE_NAME"), res.getString("TABLE_TYPE"),
                    res.getString("REMARKS"));
        }

        if (userTableExists && applicationTableExists && jobTableExists) {
            return true;
        } else if (!userTableExists && !applicationTableExists && !jobTableExists) {
            return false;
        } else {
            throw new RuntimeException("Database is partially ready: quitting");
        }
    }

    static void maybeCreateTables(Connection conn) throws SQLException, IOException {
        LOG.info("Checking database schema of {} ...", databaseURL);

        if (allTableExists(conn)) {
            LOG.info("All three table exists.");
        } else {
            LOG.info("No table exists: creating...");

            InputStream ddl = Launcher.class.getResourceAsStream("/retz-ddl.sql");
            String createString = org.apache.commons.io.IOUtils.toString(ddl, UTF_8);
            //System.err.println(createString);
            try (Statement statement = conn.createStatement()) {
                statement.execute(createString);
            }
        }
    }

    static List<User> allUsers() {
        List<User> ret = new LinkedList<>();
        //try (Connection conn = DriverManager.getConnection(databaseURL)) {
        try (Connection conn = pool.getConnection();
             PreparedStatement p = conn.prepareStatement("SELECT * FROM users")) {
            conn.setAutoCommit(true);

            try (ResultSet res = p.executeQuery()) {
                while (res.next()) {
                    User u = new User(res.getString("key_id"), res.getString("secret"), res.getBoolean("enabled"));
                    ret.add(u);
                }
            }
        } catch (SQLException e) {
            LOG.error(e.toString());
        }
        return ret;
    }

    // Maybe this must return Optional<User> ?
    public static User createUser() throws RuntimeException {

        String keyId = UUID.randomUUID().toString().replace("-", "");
        String secret = UUID.randomUUID().toString().replace("-", "");
        User u = new User(keyId, secret, true);
        LOG.info("new (key_id, secret) = ({}, {})", keyId, secret);
        if (addUser(u)) {
            return u;
        } else {
            throw new RuntimeException("Couldn't create user " + keyId);
        }
    }

    public static boolean addUser(User u) {
        //try (Connection conn = DriverManager.getConnection(databaseURL)) {
        try (Connection conn = pool.getConnection();
             PreparedStatement p = conn.prepareStatement("INSERT INTO users(key_id, secret, enabled) values(?, ?, ?)")) {
            conn.setAutoCommit(true);

            p.setString(1, u.keyId());
            p.setString(2, u.secret());
            p.setBoolean(3, true);
            p.execute();
            return true;
        } catch (SQLException e) {
            for (Throwable t : e) {
                LOG.error(t.toString());
            }
            return false;
        }
    }

    public static Optional<User> getUser(String keyId) {
        //try (Connection conn = DriverManager.getConnection(databaseURL)) {
        try (Connection conn = pool.getConnection()) {
            conn.setAutoCommit(false);
            Optional<User> u = getUser(conn, keyId);
            conn.commit();
            return u;
        } catch (SQLException e) {
            LOG.error(e.toString());
            return Optional.empty();
        }
    }

    public static Optional<User> getUser(Connection conn, String keyId) throws SQLException {
        if (conn.getAutoCommit()) {
            throw new RuntimeException("Autocommit on");
        }
        try (PreparedStatement p = conn.prepareStatement("SELECT * FROM USERS WHERE key_id = ?")) {

            p.setString(1, keyId);
            try (ResultSet res = p.executeQuery()) {
                if (res.next()) {
                    return Optional.of(new User(res.getString("key_id"), res.getString("secret"), res.getBoolean("enabled")));
                }
            }
            // User not found
            return Optional.empty();
        }
    }

    public static void enableUser(String keyId, boolean enabled) {
        throw new RuntimeException("Not yet implemented");
    }

    // public static void deleteUser(String keyId) {
    //    throw new RuntimeException("Not yet implemented");
    //}

    public static List<Application> getAllApplications() throws IOException {
        return getAllApplications(null);
    }

    public static List<Application> getAllApplications(String id) throws IOException {
        List<Application> ret = new LinkedList<>();
        try (Connection conn = pool.getConnection()) {
            conn.setAutoCommit(false);
            ret = getApplications(conn, id);
            conn.commit();
        } catch (SQLException e) {
            LOG.error(e.toString());
        }
        return ret;
    }

    public static List<Application> getApplications(Connection conn, String id) throws SQLException, IOException {
        if (conn.getAutoCommit()) {
            throw new AssertionError("autocommit must be false");
        }
        List<Application> ret = new LinkedList<>();
        String sql = "SELECT * FROM applications";
        if (id != null) {
            sql += " WHERE owner=?";
        }
        try (PreparedStatement p = conn.prepareStatement(sql)) {
            if (id != null) {
                p.setString(1, id);
            }
            try (ResultSet res = p.executeQuery()) {
                while (res.next()) {
                    String json = res.getString("json");
                    Application app = MAPPER.readValue(json, Application.class);
                    ret.add(app);
                }
            }
        }
        return ret;
    }

    public static boolean addApplication(Application a) throws JsonProcessingException {
        try (Connection conn = pool.getConnection();
             PreparedStatement p = conn.prepareStatement("INSERT INTO applications(appid, owner, json) values(?, ?, ?)")) {
            conn.setAutoCommit(false);

            Optional<User> u = getUser(conn, a.getOwner());
            if (!u.isPresent() || !u.get().enabled()) {
                LOG.warn("{} tried to load application {} while {} is disabled or not present",
                        a.getOwner(), a.getAppid(), a.getOwner());
                return false;
            }

            deleteApplication(conn, a.getAppid());
            p.setString(1, a.getAppid());
            p.setString(2, a.getOwner());
            p.setString(3, MAPPER.writeValueAsString(a));
            p.execute();
            conn.commit();
            return true;

        } catch (SQLException e) {
            LOG.error(e.toString());
            return false;
        }
    }

    public static Optional<Application> getApplication(String appid) throws IOException {
        try (Connection conn = pool.getConnection()) {
            conn.setAutoCommit(true);
            return getApplication(conn, appid);
        } catch (SQLException e) {
            LOG.error(e.toString());
        }
        return Optional.empty();
    }

    public static Optional<Application> getApplication(Connection conn, String appid) throws SQLException, IOException {
        try (PreparedStatement p = conn.prepareStatement("SELECT * FROM applications WHERE appid = ?")) {

            p.setString(1, appid);
            try (ResultSet res = p.executeQuery()) {
                if (res.next()) {
                    String json = res.getString("json");
                    Application app = MAPPER.readValue(json, Application.class);
                    if (!appid.equals(app.getAppid())) {
                        LOG.error("{} != {} in Database", appid, app.getAppid());
                        throw new AssertionError("Appid in JSON must be equal to the column");
                    }
                    return Optional.of(app);
                }
                // No such application
            }
        }
        return Optional.empty();
    }

    public static void safeDeleteApplication(String appid) {
        try (Connection conn = pool.getConnection()) {
            conn.setAutoCommit(false);
            // TODO: check there are no non-finished Jobs
            // TODO: THINK: what about finished jobs??????
            deleteApplication(conn, appid);
            LOG.info("commiting deletion... {}", appid);

            conn.commit();
        } catch (SQLException e) {
            LOG.error(e.toString());
        }
    }

    // Within transaction context and autocommit must be false
    public static void deleteApplication(Connection conn, String appid) throws SQLException {
        if (conn.getAutoCommit()) {
            throw new AssertionError("autocommit must be false");
        }
        try (PreparedStatement p = conn.prepareStatement("DELETE FROM applications where appid=?")) {
            p.setString(1, appid);
            p.execute();
        }
    }

    public static List<Job> getAllJobs(String id) throws IOException {
        List<Job> ret = new LinkedList<>();
        String sql = "SELECT jobs.json FROM jobs";
        if (id != null) {
            sql = "SELECT jobs.json FROM jobs, applications WHERE jobs.appid = applications.appid AND applications.owner = ?";
        }
        try (Connection conn = pool.getConnection();
             PreparedStatement p = conn.prepareStatement(sql)) {
            if( id != null) {
                p.setString(1, id);
            }
            conn.setAutoCommit(true);

            try (ResultSet res = p.executeQuery()) {
                while (res.next()) {
                    String json = res.getString("jobs.json");
                    Job job = MAPPER.readValue(json, Job.class);
                    ret.add(job);
                }
            }
        } catch (SQLException e) {
            LOG.error(e.toString());
        }
        return ret;
    }

    // Selects all "finished" jobs
    public static List<Job> finishedJobs(String id, String start, String end) {
        List<Job> ret = new LinkedList<>();
        try (Connection conn = pool.getConnection();
             PreparedStatement p = conn.prepareStatement("SELECT * FROM jobs WHERE owner=? AND ? <= finished AND finished < ?")) {
            conn.setAutoCommit(true);

            try (ResultSet res = p.executeQuery()) {

                while(res.next()) {
                    String json = res.getString("json");
                    try {
                        Job job = MAPPER.readValue(json, Job.class);
                        if (job == null) {
                            throw new AssertionError("Cannot be null!!");
                        }
                        ret.add(job);
                    } catch (IOException e) {
                        LOG.error(e.toString()); // JSON text is broken for sure
                    }
                }
            }
        } catch (SQLException e) {
            LOG.error(e.toString());
        }
        return ret;
    }

    static List<Job> findFit(int cpu, int memMB) throws IOException {
        List<Job> ret = new LinkedList<>();
        try (Connection conn = pool.getConnection();
             PreparedStatement p = conn.prepareStatement("SELECT * FROM jobs WHERE state='QUEUED'")) {
            conn.setAutoCommit(true);

            try (ResultSet res = p.executeQuery()) {
                int totalCpu = 0;
                int totalMem = 0;

                while (res.next() && totalCpu <= cpu && totalMem <= memMB) {
                    String json = res.getString("json");
                    Job job = MAPPER.readValue(json, Job.class);

                    if (job == null) {
                        throw new AssertionError("Cannot be null!!");
                    } else if (totalCpu + job.cpu() <= cpu && totalMem + job.memMB() <= memMB) {
                        ret.add(job);
                        totalCpu += job.cpu();
                        totalMem += job.memMB();
                    } else {
                        break;
                    }
                }
            }
        } catch (SQLException e) {
            LOG.error(e.toString());
        }
        return ret;
    }

    public static void addJob(Connection conn, Job j) throws SQLException, JsonProcessingException {
        try (PreparedStatement p = conn.prepareStatement("INSERT INTO jobs(name, id, appid, cmd, taskid, state, json) values(?, ?, ?, ?, ?, ?, ?)")) {
            p.setString(1, j.name());
            p.setInt(2, j.id());
            p.setString(3, j.appid());
            p.setString(4, j.cmd());
            p.setString(5, j.taskId());
            p.setString(6, j.state().toString());
            p.setString(7, MAPPER.writeValueAsString(j));
            p.execute();
        }
    }

    static void updateJob(Connection conn, Job j) throws SQLException, JsonProcessingException {
        LOG.debug("Updating job as name={}, id={}, appid={}", j.name(), j.id(), j.appid());
        try (PreparedStatement p = conn.prepareStatement("UPDATE jobs SET name=?, appid=?, cmd=?, taskid=?, state=?, started=?, finished=?, json=? WHERE id=?")) {
            p.setString(1, j.name());
            p.setString(2, j.appid());
            p.setString(3, j.cmd());
            p.setString(4, j.taskId());
            p.setString(5, j.state().toString());
            p.setString(6, j.started());
            p.setString(7, j.finished());
            p.setString(8, MAPPER.writeValueAsString(j));
            p.setInt(9, j.id());
            p.execute();
        }
    }

    public static void safeAddJob(Job j) {
        try (Connection conn = pool.getConnection()) {
            conn.setAutoCommit(false);

            Optional<Application> app = getApplication(conn, j.appid());
            if (!app.isPresent()) {
                throw new RuntimeException("No such application: " + j.appid());
            }

            addJob(conn, j);
            conn.commit();

        } catch (IOException e) {
            throw new RuntimeException(e.toString());
        } catch (SQLException e) {
            LOG.error(e.toString());
        }
    }

    public static Optional<Job> getJob(int id) throws JsonProcessingException, IOException {
        try (Connection conn = pool.getConnection();
             PreparedStatement p = conn.prepareStatement("SELECT * FROM jobs WHERE id = ?")) {
            conn.setAutoCommit(true);
            p.setInt(1, id);
            try (ResultSet res = p.executeQuery()) {
                if (res.next()) {
                    String json = res.getString("json");
                    Job job = MAPPER.readValue(json, Job.class);
                    if (id != job.id()) {
                        LOG.error("{} != {} in Database", id, job.id());
                        throw new AssertionError("id in JSON must be equal to the column");
                    }
                    return Optional.of(job);
                }
                // No such application
            }
        } catch (SQLException e) {
            LOG.error(e.toString());
        }
        return Optional.empty();
    }

    public static Optional<Job> getJobFromTaskId(String taskId) throws JsonProcessingException, IOException {
        try (Connection conn = pool.getConnection();
             PreparedStatement p = conn.prepareStatement("SELECT json FROM jobs WHERE taskid=?")) {
            conn.setAutoCommit(true);

            p.setString(1, taskId);

            try (ResultSet res = p.executeQuery()) {
                if (res.next()) {
                    String json = res.getString("json");
                    Job job = MAPPER.readValue(json, Job.class);
                    if (!taskId.equals(job.taskId())) {
                        LOG.error("{} != {} in Database", taskId, job.taskId());
                        throw new AssertionError("id in JSON must be equal to the column");
                    }
                    return Optional.of(job);
                }
                LOG.info("no such application/job");
                // No such application
            }
        } catch (SQLException e) {
            LOG.error(e.toString());
        }
        return Optional.empty();
    }

    // Delete all jobs that has ID smaller than id
    public static void deleteAllJob(int maxId) {
        try (Connection conn = pool.getConnection();
             PreparedStatement p = conn.prepareStatement("DELETE FROM jobs WHERE id < ?")) {
            conn.setAutoCommit(true);
            p.setInt(1, maxId);
            p.execute();
        } catch (SQLException e) {
            LOG.error(e.toString());
        }
    }

    public static void setJobStarting(int id, Optional<String> maybeUrl, String taskId) {
        updateJob(id, job -> {
            job.starting(taskId, maybeUrl, TimestampHelper.now());
            LOG.info("TaskId of id={}: {} / {}", id, taskId, job.taskId());
            return Optional.of(job);
        });
    }

    static void updateJob(int id, Function<Job, Optional<Job>> fun) {
        try (Connection conn = pool.getConnection();
             PreparedStatement p = conn.prepareStatement("SELECT json FROM jobs WHERE id=?")) {
            conn.setAutoCommit(false);
            p.setInt(1, id);
            try (ResultSet set = p.executeQuery()) {
                if (set.next()) {
                    String json = set.getString("json");
                    Job job = MAPPER.readValue(json, Job.class);
                    Optional<Job> result = fun.apply(job);
                    if (result.isPresent()) {
                        // addJob..
                        updateJob(conn, job);
                        conn.commit();
                        LOG.info("Job (id={}) status updated to {}", job.id(), job.state());
                    }
                } else {
                    LOG.warn("No such job: id={}", id);
                }
            } catch (IOException e) {
                LOG.error(e.toString());
            }
        } catch (SQLException e) {
            LOG.error(e.toString());
        }
    }

    public static int countJobs() {
        try (Connection conn = pool.getConnection();
             PreparedStatement p = conn.prepareStatement("SELECT count(id) FROM jobs")) {
            conn.setAutoCommit(true);
            try (ResultSet set = p.executeQuery()) {
                if (set.next()) {
                    return set.getInt(1);
                }
            }
        } catch (SQLException e) {
            LOG.error(e.toString());
        }
        return -1;
    }

    static int countRunning() {
        try (Connection conn = pool.getConnection();
             PreparedStatement p = conn.prepareStatement("SELECT count(id) FROM jobs WHERE state = ?")) {
            conn.setAutoCommit(true);
            p.setString(1, Job.JobState.STARTED.toString());
            try (ResultSet set = p.executeQuery()) {
                if (set.next()) {
                    return set.getInt(1);
                }
            }
        } catch (SQLException e) {
            LOG.error(e.toString());
        }
        return -1;
    }

    public static int getLatestJobId() {
        try (Connection conn = pool.getConnection();
             PreparedStatement p = conn.prepareStatement("SELECT id FROM jobs ORDER BY id DESC LIMIT 1")) {
            conn.setAutoCommit(true);
            try (ResultSet res = p.executeQuery()) {
                if (res.next()) {
                    int id = res.getInt("id");
                    return id;
                }
                // No such application
            }
        } catch (SQLException e) {
            LOG.error(e.toString());
        }
        return 0;
    }
}
