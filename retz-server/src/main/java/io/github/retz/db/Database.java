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
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import io.github.retz.cli.TimestampHelper;
import io.github.retz.db.migration.DBMigration;
import io.github.retz.misc.LogUtil;
import io.github.retz.planner.AppJobPair;
import io.github.retz.protocol.data.Application;
import io.github.retz.protocol.data.Job;
import io.github.retz.protocol.data.User;
import io.github.retz.protocol.exception.JobNotFoundException;
import io.github.retz.scheduler.ServerConfiguration;
import org.apache.tomcat.jdbc.pool.DataSource;
import org.apache.tomcat.jdbc.pool.PoolProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.*;
import java.text.MessageFormat;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public class Database {
    private static final Logger LOG = LoggerFactory.getLogger(Database.class);
    private static Database database = new Database();

    private final ObjectMapper mapper = new ObjectMapper();
    private final DataSource dataSource = new DataSource();
    private final DBMigration dbMigrator = new DBMigration((javax.sql.DataSource) dataSource);
    String databaseURL = null;

    Database() {
        mapper.registerModule(new Jdk8Module());
    }

    public static Database getInstance() {
        return database;
    }

    public static DataSource getDataSource() {
        return getInstance().dataSource;
    }

    public static DBMigration getMigrator() {
        return getInstance().dbMigrator;
    }

    static Database newMemInstance(String name) throws IOException {
        Database db = new Database();
        db.initOnMem(name);
        return db;
    }

    public void validate() throws Exception {
        Objects.requireNonNull(databaseURL);
        Objects.requireNonNull(dataSource);
        try (Connection conn = dataSource.getConnection();
             Statement s = conn.createStatement();
             ResultSet r = s.executeQuery("select 1")) {
            if (!r.next()) {
                throw new AssertionError("Database is not ready: result has no next");
            }
        }
    }

    public void init(ServerConfiguration config) throws IOException {
        databaseURL = Objects.requireNonNull(config.getDatabaseURL());
        LOG.info("Initializing database {}", databaseURL);

        PoolProperties props = new PoolProperties();

        props.setUrl(config.getDatabaseURL());
        props.setDriverClassName(config.getDatabaseDriver());
        if (config.getDatabaseUser().isPresent()) {
            props.setUsername(config.getDatabaseUser().get());
            if (config.getDatabasePass().isPresent()) {
                props.setPassword(config.getDatabasePass().get());
            }
        }

        init(props, true);

        if (getUser(config.getAccessKey()).isPresent()) {
            LOG.info("admin user is {}", config.getAccessKey());
        } else {
            LOG.info("No user found: creating admin user {}", config.getAccessKey());
            addUser(config.getUser());
        }
    }

    void initOnMem(String name) throws IOException {
        PoolProperties props = new PoolProperties();
        databaseURL = "jdbc:h2:mem:" + name + ";DB_CLOSE_DELAY=-1";
        props.setUrl(databaseURL);
        props.setDriverClassName("org.h2.Driver");
        LOG.info("URL={}, Driver={}", props.getUrl(), props.getDriverClassName());
        init(props, false);
    }

    private void init(PoolProperties props, boolean enableJmx) throws IOException {
        props.setValidationQuery("select 1;");
        props.setJmxEnabled(enableJmx);

        dataSource.setPoolProperties(props);

        try {
            dbMigrator.migrate();
        } catch (SQLException | IOException e) {
            throw new IOException("Database.init() failed", e);
        }
    }

    public void stop() {
        LOG.info("Stopping database {}", databaseURL);

        while (dataSource.getNumActive() > 0) {
            try {
                Thread.sleep(512);
                LOG.info("Stopping database: active/idle ={}/{}", dataSource.getNumActive(), dataSource.getNumIdle());
            } catch (InterruptedException e) {
            }
        }
        dataSource.close();
        LOG.info("Stopped database");
    }

    // This is for test purpose
    public void clear() {
        try {
            dbMigrator.clean();
            LOG.info("All tables dropped successfully");
        } catch (IOException e) {
            LogUtil.error(LOG,"Database.clear() failed", e);
        }
    }


    public List<User> allUsers() throws IOException {
        List<User> ret = new ArrayList<>();
        //try (Connection conn = DriverManager.getConnection(databaseURL)) {
        //try (Connection conn = pool.getConnection();
        try (Connection conn = dataSource.getConnection(); //pool.getConnection()) {
             PreparedStatement p = conn.prepareStatement("SELECT * FROM users")) {
            conn.setAutoCommit(true);

            try (ResultSet res = p.executeQuery()) {
                while (res.next()) {
                    User u = mapper.readValue(res.getString("json"), User.class);
                    ret.add(u);
                }
            }
            return ret;
        } catch (SQLException | IOException e) {
            throw new IOException("Database.allUsers() failed", e);
        }
    }

    // Maybe this must return Optional<User> ?
    public User createUser(String info) throws IOException {
        String keyId = UUID.randomUUID().toString().replace("-", "");
        String secret = UUID.randomUUID().toString().replace("-", "");
        User u = new User(keyId, secret, true, info);
        LOG.info("new (key_id, secret) = ({}, {})", keyId, secret);
        addUser(u);
        return u;
    }

    public boolean addUser(User u) throws IOException {
        //try (Connection conn = DriverManager.getConnection(databaseURL)) {
        try (Connection conn = dataSource.getConnection(); //pool.getConnection();
             PreparedStatement p = conn.prepareStatement("INSERT INTO users(key_id, secret, enabled, json) values(?, ?, ?, ?)")) {
            conn.setAutoCommit(true);

            p.setString(1, u.keyId());
            p.setString(2, u.secret());
            p.setBoolean(3, true);
            p.setString(4, mapper.writeValueAsString(u));
            p.execute();
            return true;
        } catch (SQLException | IOException e) {
            throw new IOException(MessageFormat.format("Database.addUser({0}) failed", u.keyId()), e);
        }
    }

    private void updateUser(Connection conn, User updatedUser) throws SQLException, JsonProcessingException {
        try (PreparedStatement p = conn.prepareStatement("UPDATE users SET secret=?, enabled=?, json=? WHERE key_id=?")) {
            p.setString(1, updatedUser.secret());
            p.setBoolean(2, updatedUser.enabled());
            p.setString(3, mapper.writeValueAsString(updatedUser));
            p.setString(4, updatedUser.keyId());
            p.execute();
        }
    }

    public Optional<User> getUser(String keyId) throws IOException {
        //try (Connection conn = DriverManager.getConnection(databaseURL)) {
        try (Connection conn = dataSource.getConnection()) { //pool.getConnection()) {
            conn.setAutoCommit(false);
            Optional<User> u = getUser(conn, keyId);
            conn.commit();
            return u;
        } catch (SQLException | IOException e) {
            throw new IOException(MessageFormat.format("Database.getUser({0}) failed", keyId), e);
        }
    }

    private Optional<User> getUser(Connection conn, String keyId) throws SQLException, IOException {
        if (conn.getAutoCommit()) {
            throw new AssertionError("autocommit must be false");
        }
        try (PreparedStatement p = conn.prepareStatement("SELECT * FROM USERS WHERE key_id = ?")) {

            p.setString(1, keyId);
            try (ResultSet res = p.executeQuery()) {
                if (res.next()) {
                    User u = mapper.readValue(res.getString("json"), User.class);
                    return Optional.of(u);
                }
            }
            // User not found
            return Optional.empty();
        }
    }

    public void enableUser(String keyId, boolean enabled) throws IOException {
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            Optional<User> user = getUser(conn, keyId);
            if (user.isPresent()) {
                user.get().enable(enabled);
                updateUser(conn, user.get());
            }
            conn.commit();
        } catch (SQLException | IOException e) {
            throw new IOException(MessageFormat.format("Database.enableUser({0}) failed", keyId), e);
        }
    }

    public List<Application> getAllApplications() throws IOException {
        return getAllApplications(null);
    }

    public List<Application> getAllApplications(String id) throws IOException {
        List<Application> ret = Collections.emptyList();
        try (Connection conn = dataSource.getConnection()) { //pool.getConnection()) {
            conn.setAutoCommit(false);
            ret = getApplications(conn, id);
            conn.commit();
            return ret;
        } catch (SQLException | IOException e) {
            throw new IOException(MessageFormat.format("Database.getAllApplications({0}) failed", id), e);
        }
    }

    private List<Application> getApplications(Connection conn, String id) throws SQLException, IOException {
        if (conn.getAutoCommit()) {
            throw new AssertionError("autocommit must be false");
        }
        List<Application> ret = new ArrayList<>();
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
                    Application app = mapper.readValue(json, Application.class);
                    ret.add(app);
                }
            }
        }
        return ret;
    }

    public boolean addApplication(Application a) throws IOException {
        try (Connection conn = dataSource.getConnection(); //pool.getConnection();
             PreparedStatement p = conn.prepareStatement("INSERT INTO applications(appid, owner, json) values(?, ?, ?)")) {
            conn.setAutoCommit(false);

            Optional<User> u = getUser(conn, a.getOwner());
            if (!u.isPresent()) {
                LOG.warn("{} tried to load application {}, but the user not present",
                        a.getOwner(), a.getAppid());
                return false;
            } else if (!u.get().enabled()) {
                LOG.warn("{} tried to load application {}, but user.enabled={}",
                        a.getOwner(), a.getAppid(), u.get().enabled());
                return false;
            }

            deleteApplication(conn, a.getAppid());
            p.setString(1, a.getAppid());
            p.setString(2, a.getOwner());
            p.setString(3, mapper.writeValueAsString(a));
            p.execute();
            conn.commit();
            return true;

        } catch (SQLException | IOException e) {
            throw new IOException(
                    MessageFormat.format("Database.addApplication({0}, {1}) failed", a.getAppid(), a.getOwner()), e);
        }
    }

    public Optional<Application> getApplication(String appid) throws IOException {
        try (Connection conn = dataSource.getConnection()) { //pool.getConnection()) {
            conn.setAutoCommit(true);
            return getApplication(conn, appid);
        } catch (SQLException e) {
            throw new IOException(MessageFormat.format("Database.getApplication({0}) failed", appid), e);
        }
    }

    private Optional<Application> getApplication(Connection conn, String appid) throws SQLException, IOException {
        try (PreparedStatement p = conn.prepareStatement("SELECT * FROM applications WHERE appid = ?")) {

            p.setString(1, appid);
            try (ResultSet res = p.executeQuery()) {
                if (res.next()) {
                    String json = res.getString("json");
                    Application app = mapper.readValue(json, Application.class);
                    if (!appid.equals(app.getAppid())) {
                        LOG.error("{} != {} in Database", appid, app.getAppid());
                        throw new AssertionError("Appid in JSON must be equal to the column");
                    }
                    return Optional.of(app);
                }
                // No such application
                return Optional.empty();
            }
        }
    }

    public void safeDeleteApplication(String appid) throws IOException {
        try (Connection conn = dataSource.getConnection()) { //pool.getConnection()) {
            conn.setAutoCommit(false);
            // TODO: check there are no non-finished Jobs
            // TODO: THINK: what about finished jobs??????
            deleteApplication(conn, appid);
            LOG.info("commiting deletion... {}", appid);

            conn.commit();
        } catch (SQLException e) {
            throw new IOException(MessageFormat.format("Database.safeDeleteApplication({0}) failed", appid), e);
        }
    }

    // Within transaction context and autocommit must be false
    private void deleteApplication(Connection conn, String appid) throws SQLException {
        if (conn.getAutoCommit()) {
            throw new AssertionError("autocommit must be false");
        }
        try (PreparedStatement p = conn.prepareStatement("DELETE FROM applications where appid=?")) {
            p.setString(1, appid);
            p.execute();
        }
    }

    public List<Job> listJobs(String id, Job.JobState state, Optional<String> tag, int limit) throws IOException {
        List<Job> ret = new ArrayList<>();
        String prefix = "SELECT j.json FROM jobs j, applications a WHERE j.appid = a.appid AND a.owner = ?";
        String sql = prefix + " AND j.state=? ORDER BY j.id DESC LIMIT ?";

        try (Connection conn = dataSource.getConnection(); // pool.getConnection();
             PreparedStatement p = conn.prepareStatement(sql)) {
            p.setString(1, id);
            p.setString(2, state.toString());
            p.setInt(3, limit);

            conn.setAutoCommit(true);

            try (ResultSet res = p.executeQuery()) {
                while (res.next()) {
                    String json = res.getString(1);
                    Job job = mapper.readValue(json, Job.class);
                    assert job.state() == state;
                    if (tag.isPresent() && !job.tags().contains(tag.get())) {
                        continue;
                    }
                    ret.add(job);
                }
            }
            return ret;
        } catch (SQLException e) {
            throw new IOException(MessageFormat.format("Database.listJobs({0}, {1}) failed", id, state), e);
        }
    }

    // This is for debug purpose
    List<Job> getAllJobs(String id) throws IOException {
        List<Job> ret = new ArrayList<>();
        String sql = "SELECT j.json FROM jobs j";
        if (id != null) {
            sql = "SELECT j.json FROM jobs j, applications a WHERE j.appid = a.appid AND a.owner = ?";
        }
        try (Connection conn = dataSource.getConnection(); // pool.getConnection();
             PreparedStatement p = conn.prepareStatement(sql)) {
            if (id != null) {
                p.setString(1, id);
            }
            conn.setAutoCommit(true);

            try (ResultSet res = p.executeQuery()) {
                while (res.next()) {
                    //String json = res.getString("j.json");
                    String json = res.getString(1);
                    Job job = mapper.readValue(json, Job.class);
                    ret.add(job);
                }
            }
        } catch (SQLException e) {
            throw new IOException("Database.getAllJobs() failed", e);
        }
        return ret;
    }

    // Selects all "finished" jobs
    public List<Job> finishedJobs(String start, String end) throws IOException {
        List<Job> ret = new ArrayList<>();
        try (Connection conn = dataSource.getConnection(); //pool.getConnection();
             PreparedStatement p = conn.prepareStatement("SELECT * FROM jobs WHERE ? <= finished AND finished < ?")) {
            conn.setAutoCommit(true);

            p.setString(1, start);
            p.setString(2, end);
            try (ResultSet res = p.executeQuery()) {
                while (res.next()) {
                    String json = res.getString("json");
                    Job job = mapper.readValue(json, Job.class);
                    if (job == null) {
                        throw new AssertionError("Cannot be null!!");
                    }
                    ret.add(job);
                }
            }
            return ret;
        } catch (SQLException | IOException e) {
            throw new IOException(MessageFormat.format("Database.finishedJobs({0}, {1}) failed", start, end), e);
        }
    }

    // orderBy must not have any duplication
    public List<Job> findAll(List<String> orderBy, int limit) throws IOException {
        List<Job> ret = new ArrayList<>();
        String orders = orderBy.stream().map(s -> s + " ASC").collect(Collectors.joining(", "));
        String sql = "SELECT * FROM jobs WHERE state='QUEUED' ORDER BY " + orders;
        if (limit >= 0) {
            sql += " LIMIT " + limit;
        }
        try (Connection conn = dataSource.getConnection(); //pool.getConnection();
             PreparedStatement p = conn.prepareStatement(sql)) {
            try (ResultSet res = p.executeQuery()) {
                while (res.next()) {
                    String json = res.getString("json");
                    Job job = mapper.readValue(json, Job.class);
                    if (job == null) {
                        throw new AssertionError("Cannot be null!!");
                    }
                    ret.add(job);
                }
            }
            return ret;
        } catch (SQLException | IOException e) {
            throw new IOException("Database.findAll() failed", e);
        }
    }

    public List<Job> findFit(List<String> orderBy, int cpu, int memMB) throws IOException {
        List<Job> ret = new ArrayList<>();
        String orders = orderBy.stream().map(s -> s + " ASC").collect(Collectors.joining(", "));
        try (Connection conn = dataSource.getConnection(); //pool.getConnection();
             PreparedStatement p = conn.prepareStatement("SELECT * FROM jobs WHERE state='QUEUED' ORDER BY " + orders)) {
            conn.setAutoCommit(true);

            try (ResultSet res = p.executeQuery()) {
                int totalCpu = 0;
                int totalMem = 0;

                while (res.next() && totalCpu <= cpu && totalMem <= memMB) {
                    String json = res.getString("json");
                    Job job = mapper.readValue(json, Job.class);

                    if (job == null) {
                        throw new AssertionError("Cannot be null!!");
                    } else if (totalCpu + job.resources().getCpu() <= cpu && totalMem + job.resources().getMemMB() <= memMB) {
                        ret.add(job);
                        totalCpu += job.resources().getCpu();
                        totalMem += job.resources().getMemMB();
                    } else {
                        break;
                    }
                }
            }
            return ret;
        } catch (SQLException | IOException e) {
            throw new IOException(MessageFormat.format("Database.findFit({0}, {1}) failed", cpu, memMB), e);
        }
    }

    public List<Job> queued(int limit) throws IOException {
        List<Job> ret = new ArrayList<>();
        try (Connection conn = dataSource.getConnection(); //pool.getConnection();
             PreparedStatement p = conn.prepareStatement("SELECT * FROM jobs WHERE state='QUEUED' ORDER BY id ASC LIMIT ?")) {
            conn.setAutoCommit(true);
            p.setInt(1, limit);

            try (ResultSet res = p.executeQuery()) {
                while (res.next()) {
                    String json = res.getString("json");
                    Job job = mapper.readValue(json, Job.class);

                    if (job == null || job.state() != Job.JobState.QUEUED) {
                        throw new AssertionError("Cannot be null!!");
                    } else {
                        ret.add(job);
                    }
                }
            }
            return ret;
        } catch (SQLException | IOException e) {
            throw new IOException("Database.queued() failed", e);
        }
    }

    private void addJob(Connection conn, Job j) throws SQLException, JsonProcessingException {
        try (PreparedStatement p = conn.prepareStatement("INSERT INTO jobs(name, id, appid, priority, taskid, state, json) values(?, ?, ?, ?, ?, ?, ?)")) {
            p.setString(1, j.name());
            p.setInt(2, j.id());
            p.setString(3, j.appid());
            p.setInt(4, j.priority());
            p.setString(5, j.taskId());
            p.setString(6, j.state().toString());
            p.setString(7, mapper.writeValueAsString(j));
            p.execute();
        }
    }

    public void safeAddJob(Job j) throws IOException {
        try (Connection conn = dataSource.getConnection()) { //pool.getConnection()) {
            conn.setAutoCommit(false);

            Optional<Application> app = getApplication(conn, j.appid());
            if (!app.isPresent()) {
                throw new IllegalStateException("No such application: " + j.appid());
            }

            addJob(conn, j);
            conn.commit();

        } catch (SQLException | IOException e) {
            throw new IOException(MessageFormat.format("Database.safeAddJob({0}) failed", j.appid()), e);
        }
    }

    public Optional<AppJobPair> getAppJob(int id) throws IOException {
        try (Connection conn = dataSource.getConnection(); //pool.getConnection();
             PreparedStatement p = conn.prepareStatement("SELECT j.json, a.json FROM jobs j, applications a WHERE id = ? AND j.appid = a.appid")) {
            conn.setAutoCommit(true);
            p.setInt(1, id);
            try (ResultSet res = p.executeQuery()) {
                if (res.next()) {
                    String jjson = res.getString(1);
                    Job job = mapper.readValue(jjson, Job.class);
                    if (id != job.id()) {
                        LOG.error("{} != {} in Database", id, job.id());
                        throw new AssertionError("id in JSON must be equal to the column");
                    }
                    String ajson = res.getString(2);
                    Application app = mapper.readValue(ajson, Application.class);

                    return Optional.of(new AppJobPair(Optional.of(app), job));
                }
                // No such application
                return Optional.empty();
            }
        } catch (SQLException | IOException e) {
            throw new IOException(MessageFormat.format("Database.getAppJob({0}) failed", id), e);
        }
    }

    public Optional<Job> getJob(int id) throws IOException {
        try (Connection conn = dataSource.getConnection(); //pool.getConnection();
             PreparedStatement p = conn.prepareStatement("SELECT * FROM jobs WHERE id = ?")) {
            conn.setAutoCommit(true);
            p.setInt(1, id);
            try (ResultSet res = p.executeQuery()) {
                if (res.next()) {
                    String json = res.getString("json");
                    Job job = mapper.readValue(json, Job.class);
                    if (id != job.id()) {
                        LOG.error("{} != {} in Database", id, job.id());
                        throw new AssertionError("id in JSON must be equal to the column");
                    }
                    return Optional.of(job);
                }
                // No such application
                return Optional.empty();
            }
        } catch (SQLException | IOException e) {
            throw new IOException(MessageFormat.format("Database.getJob({0}) failed", id), e);
        }
    }

    public Optional<Job> getJobFromTaskId(String taskId) throws IOException {
        try (Connection conn = dataSource.getConnection(); //pool.getConnection();
             PreparedStatement p = conn.prepareStatement("SELECT json FROM jobs WHERE taskid=?")) {
            conn.setAutoCommit(true);

            p.setString(1, taskId);
            try (ResultSet res = p.executeQuery()) {
                if (res.next()) {
                    String json = res.getString("json");
                    Job job = mapper.readValue(json, Job.class);
                    if (!taskId.equals(job.taskId())) {
                        LOG.error("{} != {} in Database", taskId, job.taskId());
                        throw new AssertionError("id in JSON must be equal to the column");
                    }
                    return Optional.of(job);
                }
            }

            LOG.info("no such application/job");
            return Optional.empty();
        } catch (SQLException | IOException e) {
            throw new IOException(MessageFormat.format("Database.getJobFromTaskId({0}) failed", taskId), e);
        }
    }

    // Delete all jobs that has ID smaller than id
    public void deleteAllJob(int maxId) throws IOException {
        try (Connection conn = dataSource.getConnection(); //pool.getConnection();
             PreparedStatement p = conn.prepareStatement("DELETE FROM jobs WHERE id < ?")) {
            conn.setAutoCommit(true);
            p.setInt(1, maxId);
            p.execute();
        } catch (SQLException e) {
            throw new IOException(MessageFormat.format("Database.deleteAllJob({0}) failed", maxId), e);
        }
    }

    public void deleteOldJobs(int leeway) throws IOException {
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            new Jobs(conn, mapper).collect(leeway);
        } catch (SQLException e) {
            throw new IOException(MessageFormat.format("Database.deleteOldJobs({0}) failed", leeway), e);
        }
    }

    public void setJobStarting(int id, Optional<String> maybeUrl, String taskId) throws IOException, JobNotFoundException {
        updateJob(id, job -> {
            job.starting(taskId, maybeUrl, TimestampHelper.now());
            LOG.info("TaskId of id={}: {} / {}", id, taskId, job.taskId());
            return Optional.of(job);
        });
    }

    public void updateJob(int id, Function<Job, Optional<Job>> fun) throws IOException, JobNotFoundException {
        try (Connection conn = dataSource.getConnection(); //pool.getConnection();
             PreparedStatement p = conn.prepareStatement("SELECT json FROM jobs WHERE id=?")) {
            conn.setAutoCommit(false);
            p.setInt(1, id);
            try (ResultSet set = p.executeQuery()) {
                if (set.next()) {
                    String json = set.getString("json");
                    Job job = mapper.readValue(json, Job.class);
                    Optional<Job> result = fun.apply(job);
                    if (result.isPresent()) {
                        // addJob..
                        new Jobs(conn, mapper).updateJob(job);
                        conn.commit();
                        LOG.info("Job (id={}) status updated to {}", job.id(), job.state());
                    }
                } else {
                    throw new JobNotFoundException(id);
                }
            }
        } catch (SQLException | IOException e) {
            throw new IOException(MessageFormat.format("Database.updateJob({0}) failed", id), e);
        }
    }

    public int countJobs() throws IOException {
        try (Connection conn = dataSource.getConnection(); //pool.getConnection();
             PreparedStatement p = conn.prepareStatement("SELECT count(id) FROM jobs")) {
            conn.setAutoCommit(true);
            try (ResultSet set = p.executeQuery()) {
                if (set.next()) {
                    return set.getInt(1);
                }
            }
            return -1;
        } catch (SQLException e) {
            throw new IOException("Database.countJobs() failed", e);
        }
    }

    public int countRunning() throws IOException {
        return countByState(Job.JobState.STARTED) + countByState(Job.JobState.STARTING);
    }

    public int countQueued() throws IOException {
        return countByState(Job.JobState.QUEUED);
    }

    private int countByState(Job.JobState state) throws IOException {
        try (Connection conn = dataSource.getConnection(); //pool.getConnection();
             PreparedStatement p = conn.prepareStatement("SELECT count(id) FROM jobs WHERE state = ?")) {
            conn.setAutoCommit(true);

            p.setString(1, state.toString());
            try (ResultSet set = p.executeQuery()) {
                if (set.next()) {
                    return set.getInt(1);
                }
                return 0;
            }
        } catch (SQLException e) {
            throw new IOException(MessageFormat.format("Database.countByState({0}) failed", state), e);
        }
    }

    public int getLatestJobId() throws IOException {
        try (Connection conn = dataSource.getConnection(); //pool.getConnection();
             PreparedStatement p = conn.prepareStatement("SELECT id FROM jobs ORDER BY id DESC LIMIT 1")) {
            conn.setAutoCommit(true);
            try (ResultSet res = p.executeQuery()) {
                if (res.next()) {
                    int id = res.getInt("id");
                    return id;
                }
            }
            // No such application
            return 0;
        } catch (SQLException e) {
            throw new IOException("Database.getLatestJobId() failed", e);
        }
    }

    public List<Job> getRunning() throws IOException {
        List<Job> jobs = new ArrayList<>(2);
        jobs.addAll(getByState(Job.JobState.STARTING));
        jobs.addAll(getByState(Job.JobState.STARTED));
        return jobs;
    }

    private List<Job> getByState(Job.JobState state) throws IOException {
        List<Job> jobs = new ArrayList<>();
        try (Connection conn = dataSource.getConnection(); //pool.getConnection();
             PreparedStatement p = conn.prepareStatement("SELECT id, json FROM jobs WHERE state = ?")) {
            conn.setAutoCommit(true);

            p.setString(1, state.toString());
            try (ResultSet set = p.executeQuery()) {
                while (set.next()) {
                    String json = set.getString("json");
                    Job job = mapper.readValue(json, Job.class);
                    jobs.add(job);
                }
            }
            return jobs;
        } catch (SQLException e) {
            throw new IOException(MessageFormat.format("Database.getByState({0}) failed", state), e);
        }
    }

    public boolean setFrameworkId(String value) throws IOException {
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(true);
            LOG.info("setting new framework: {}", value);
            return new Property(conn).setFrameworkId(value);
        } catch (SQLException e) {
            throw new IOException(MessageFormat.format("Database.setFrameworkId({0}) failed", value), e);
        }
    }

    public Optional<String> getFrameworkId() throws IOException {
        try (Connection conn = dataSource.getConnection()) {
            return new Property(conn).getFrameworkId();
        } catch (SQLException e) {
            throw new IOException("Database.getFrameworkId() failed", e);
        }
    }

    public void deleteAllProperties() throws IOException {
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            new Property(conn).deleteAll();
            conn.commit();
        } catch (SQLException e) {
            throw new IOException("Database.deleteAllProperties() failed", e);
        }
    }

    public void updateJobs(List<Job> list) throws IOException {
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            Jobs jobs = new Jobs(conn, mapper);
            for (Job job : list) {
                jobs.updateJob(job);
            }
            conn.commit();
        } catch (SQLException | IOException e) {
            String ids = list.stream().map(job -> job.appid()).collect(Collectors.joining(","));
            throw new IOException(MessageFormat.format("Database.updateJobs({0}) failed", ids), e);
        }
    }

    public void retryJobs(List<Integer> ids) throws IOException {
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            new Jobs(conn, mapper).doRetry(ids);
            conn.commit();
        } catch (SQLException | IOException e) {
            throw new IOException(MessageFormat.format("Database.updateJobs({0}) failed", ids), e);
        }
    }
}
