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
import io.github.retz.protocol.data.Application;
import io.github.retz.protocol.data.Job;
import io.github.retz.protocol.data.User;
import io.github.retz.protocol.exception.JobNotFoundException;
import io.github.retz.planner.AppJobPair;
import io.github.retz.scheduler.Launcher;
import io.github.retz.scheduler.ServerConfiguration;
import org.apache.tomcat.jdbc.pool.DataSource;
import org.apache.tomcat.jdbc.pool.PoolProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.nio.charset.StandardCharsets.UTF_8;

public class Database {
    private static final Logger LOG = LoggerFactory.getLogger(Database.class);
    private static Database database = new Database();

    private final ObjectMapper MAPPER = new ObjectMapper();
    private final DataSource dataSource = new DataSource();
    String databaseURL = null;

    Database() {
        MAPPER.registerModule(new Jdk8Module());
    }

    public static Database getInstance() {
        return database;
    }

    public static DataSource getDataSource() {
        return getInstance().dataSource;
    }

    static Database newMemInstance(String name) throws IOException, SQLException {
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
        }
    }

    public void init(ServerConfiguration config) throws IOException, SQLException {
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

    void initOnMem(String name) throws IOException, SQLException {
        PoolProperties props = new PoolProperties();
        databaseURL = "jdbc:h2:mem:" + name + ";DB_CLOSE_DELAY=-1";
        props.setUrl(databaseURL);
        props.setDriverClassName("org.h2.Driver");
        LOG.info("URL={}, Driver={}", props.getUrl(), props.getDriverClassName());
        init(props, false);
    }

    private void init(PoolProperties props, boolean enableJmx) throws IOException, SQLException {
        props.setValidationQuery("select 1;");
        props.setJmxEnabled(enableJmx);

        dataSource.setPoolProperties(props);

        try (Connection conn = dataSource.getConnection()) { //pool.getConnection()) {
            conn.setAutoCommit(false);
            DatabaseMetaData meta = conn.getMetaData();

            if (!meta.supportsTransactionIsolationLevel(Connection.TRANSACTION_SERIALIZABLE)) {
                LOG.error("Current database ({}) does not support required isolation level ({})",
                        databaseURL, Connection.TRANSACTION_SERIALIZABLE);
                throw new RuntimeException("Current database does not support serializable");
            }
            maybeCreateTables(conn);
            conn.commit();
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
        try (Connection conn = dataSource.getConnection();
             Statement statement = conn.createStatement()) {
            statement.execute("DROP TABLE users, jobs, applications, properties");
            //statement.execute("DELETE FROM jobs");
            //statement.execute("DELETE FROM applications");
            conn.commit();
            LOG.info("All tables dropped successfully");
        } catch (SQLException e) {
            LOG.error(e.toString(), e);
        }
    }

    public boolean allTableExists() {
        try (Connection conn = dataSource.getConnection()) {
            return allTableExists(conn);
        } catch (SQLException e) {
            LOG.error(e.toString(), e);
            return false;
        }
    }

    boolean allTableExists(Connection conn) throws SQLException {
        DatabaseMetaData meta = conn.getMetaData();

        // PostgreSQL accepts only lower case names while
        // H2DB holds such names with upper case names. WHAT IS THE HELL JDBC
        // TODO: add PostgreSQL inttest
        boolean userTableExists = tableExists(meta, "public", "users")
                || tableExists(meta, "PUBLIC", "USERS");
        boolean applicationTableExists = tableExists(meta, "public", "applications")
                || tableExists(meta, "PUBLIC", "APPLICATIONS");
        boolean jobTableExists = tableExists(meta, "public", "jobs")
                || tableExists(meta, "PUBLIC", "JOBS");
        boolean propTableExists = tableExists(meta, "public", "properties")
                || tableExists(meta, "PUBLIC", "PROPERTIES");

        if (userTableExists && applicationTableExists && jobTableExists && propTableExists) {
            return true;
        } else if (!userTableExists && !applicationTableExists && !jobTableExists && !propTableExists) {
            return false;
        } else {
            throw new RuntimeException("Database is partially ready: quitting");
        }
    }

    private boolean tableExists(DatabaseMetaData meta, String schemaPattern, String tableName) throws SQLException {
        try (ResultSet res = meta.getTables(null,
                Objects.requireNonNull(schemaPattern),
                Objects.requireNonNull(tableName),
                null)) {

            if (res.next()) {
                String name = res.getString("TABLE_NAME");
                LOG.info("category={}, schema={}, name={}, type={}, remarks={}",
                        res.getString("TABLE_CAT"), res.getString("TABLE_SCHEM"),
                        res.getString("TABLE_NAME"), res.getString("TABLE_TYPE"),
                        res.getString("REMARKS"));
                if (name != null) {
                    return name.equals(tableName);
                }
            }
        }
        return false;
    }

    void maybeCreateTables(Connection conn) throws SQLException, IOException {
        LOG.info("Checking database schema of {} ...", databaseURL);

        if (allTableExists(conn)) {
            LOG.info("All four table exists.");
        } else {
            LOG.info("No table exists: creating....");

            InputStream ddl = Launcher.class.getResourceAsStream("/retz-ddl.sql");
            String createString = org.apache.commons.io.IOUtils.toString(ddl, UTF_8);
            //System.err.println(createString);
            try (Statement statement = conn.createStatement()) {
                statement.execute(createString);
            }
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
                    User u = MAPPER.readValue(res.getString("json"), User.class);
                    ret.add(u);
                }
            }
        } catch (SQLException e) {
            LOG.error(e.toString(), e);
        }
        return ret;
    }

    // Maybe this must return Optional<User> ?
    public User createUser(String info) throws SQLException, JsonProcessingException {
        String keyId = UUID.randomUUID().toString().replace("-", "");
        String secret = UUID.randomUUID().toString().replace("-", "");
        User u = new User(keyId, secret, true, info);
        LOG.info("new (key_id, secret) = ({}, {})", keyId, secret);
        addUser(u);
        return u;
    }

    public boolean addUser(User u) throws SQLException, JsonProcessingException {
        //try (Connection conn = DriverManager.getConnection(databaseURL)) {
        try (Connection conn = dataSource.getConnection(); //pool.getConnection();
             PreparedStatement p = conn.prepareStatement("INSERT INTO users(key_id, secret, enabled, json) values(?, ?, ?, ?)")) {
            conn.setAutoCommit(true);

            p.setString(1, u.keyId());
            p.setString(2, u.secret());
            p.setBoolean(3, true);
            p.setString(4, MAPPER.writeValueAsString(u));
            p.execute();
            return true;
        }
    }

    private void updateUser(Connection conn, User updatedUser) throws SQLException, JsonProcessingException {
        try (PreparedStatement p = conn.prepareStatement("UPDATE users SET secret=?, enabled=?, json=? WHERE key_id=?")) {
            p.setString(1, updatedUser.secret());
            p.setBoolean(2, updatedUser.enabled());
            p.setString(3, MAPPER.writeValueAsString(updatedUser));
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
        } catch (SQLException e) {
            LOG.error(e.toString(), e);
            return Optional.empty();
        }
    }

    public Optional<User> getUser(Connection conn, String keyId) throws SQLException, IOException {
        if (conn.getAutoCommit()) {
            throw new RuntimeException("Autocommit on");
        }
        try (PreparedStatement p = conn.prepareStatement("SELECT * FROM USERS WHERE key_id = ?")) {

            p.setString(1, keyId);
            try (ResultSet res = p.executeQuery()) {
                if (res.next()) {
                    User u = MAPPER.readValue(res.getString("json"), User.class);
                    return Optional.of(u);
                }
            }
            // User not found
            return Optional.empty();
        }
    }

    public void enableUser(String keyId, boolean enabled) throws SQLException, IOException {
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            Optional<User> user = getUser(conn, keyId);
            if (user.isPresent()) {
                user.get().enable(enabled);
                updateUser(conn, user.get());
            }
            conn.commit();
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
        } catch (SQLException e) {
            LOG.error(e.toString(), e);
        }
        return ret;
    }

    public List<Application> getApplications(Connection conn, String id) throws SQLException, IOException {
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
                    Application app = MAPPER.readValue(json, Application.class);
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
            p.setString(3, MAPPER.writeValueAsString(a));
            p.execute();
            conn.commit();
            return true;

        } catch (SQLException e) {
            LOG.error(e.toString(), e);
            return false;
        }
    }

    public Optional<Application> getApplication(String appid) throws IOException {
        try (Connection conn = dataSource.getConnection()) { //pool.getConnection()) {
            conn.setAutoCommit(true);
            return getApplication(conn, appid);
        } catch (SQLException e) {
            LOG.error(e.toString(), e);
        }
        return Optional.empty();
    }

    public Optional<Application> getApplication(Connection conn, String appid) throws SQLException, IOException {
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

    public void safeDeleteApplication(String appid) {
        try (Connection conn = dataSource.getConnection()) { //pool.getConnection()) {
            conn.setAutoCommit(false);
            // TODO: check there are no non-finished Jobs
            // TODO: THINK: what about finished jobs??????
            deleteApplication(conn, appid);
            LOG.info("commiting deletion... {}", appid);

            conn.commit();
        } catch (SQLException e) {
            LOG.error(e.toString(), e);
        }
    }

    // Within transaction context and autocommit must be false
    public void deleteApplication(Connection conn, String appid) throws SQLException {
        if (conn.getAutoCommit()) {
            throw new AssertionError("autocommit must be false");
        }
        try (PreparedStatement p = conn.prepareStatement("DELETE FROM applications where appid=?")) {
            p.setString(1, appid);
            p.execute();
        }
    }

    public List<Job> listJobs(String id, Job.JobState state, Optional<String> tag, int limit) throws SQLException {
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
                    try {
                        Job job = MAPPER.readValue(json, Job.class);
                        assert job.state() == state;
                        if (tag.isPresent() && !job.tags().contains(tag.get())) {
                            continue;
                        }
                        ret.add(job);
                    } catch (IOException e) {
                        LOG.warn("Failed to decode json", e);
                    }
                }
            }
        }
        return ret;
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
                    Job job = MAPPER.readValue(json, Job.class);
                    ret.add(job);
                }
            }
        } catch (SQLException e) {
            LOG.error(e.toString(), e);
        }
        return ret;
    }

    // Selects all "finished" jobs
    public List<Job> finishedJobs(String start, String end) {
        List<Job> ret = new ArrayList<>();
        try (Connection conn = dataSource.getConnection(); //pool.getConnection();
             PreparedStatement p = conn.prepareStatement("SELECT * FROM jobs WHERE ? <= finished AND finished < ?")) {
            conn.setAutoCommit(true);

            p.setString(1, start);
            p.setString(2, end);
            try (ResultSet res = p.executeQuery()) {

                while (res.next()) {
                    String json = res.getString("json");
                    try {
                        Job job = MAPPER.readValue(json, Job.class);
                        if (job == null) {
                            throw new AssertionError("Cannot be null!!");
                        }
                        ret.add(job);
                    } catch (IOException e) {
                        LOG.error(e.toString(), e); // JSON text is broken for sure
                    }
                }
            }
        } catch (SQLException e) {
            LOG.error(e.toString(), e);
        }
        return ret;
    }

    // orderBy must not have any duplication
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
                    Job job = MAPPER.readValue(json, Job.class);

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
        } catch (SQLException e) {
            LOG.error(e.toString(), e);
        }
        return ret;
    }

    public List<Job> queued(int limit) throws IOException, SQLException {
        List<Job> ret = new ArrayList<>();
        try (Connection conn = dataSource.getConnection(); //pool.getConnection();
             PreparedStatement p = conn.prepareStatement("SELECT * FROM jobs WHERE state='QUEUED' ORDER BY id ASC LIMIT ?")) {
            conn.setAutoCommit(true);
            p.setInt(1, limit);

            try (ResultSet res = p.executeQuery()) {
                while (res.next()) {
                    String json = res.getString("json");
                    Job job = MAPPER.readValue(json, Job.class);

                    if (job == null || job.state() != Job.JobState.QUEUED) {
                        throw new AssertionError("Cannot be null!!");
                    } else {
                        ret.add(job);
                    }
                }
            }
        }
        return ret;
    }

    public void addJob(Connection conn, Job j) throws SQLException, JsonProcessingException {
        try (PreparedStatement p = conn.prepareStatement("INSERT INTO jobs(name, id, appid, cmd, priority, taskid, state, json) values(?, ?, ?, ?, ?, ?, ?, ?)")) {
            p.setString(1, j.name());
            p.setInt(2, j.id());
            p.setString(3, j.appid());
            p.setString(4, j.cmd());
            p.setInt(5, j.priority());
            p.setString(6, j.taskId());
            p.setString(7, j.state().toString());
            p.setString(8, MAPPER.writeValueAsString(j));
            p.execute();
        }
    }

    public void safeAddJob(Job j) {
        try (Connection conn = dataSource.getConnection()) { //pool.getConnection()) {
            conn.setAutoCommit(false);

            Optional<Application> app = getApplication(conn, j.appid());
            if (!app.isPresent()) {
                throw new RuntimeException("No such application: " + j.appid());
            }

            addJob(conn, j);
            conn.commit();

        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (SQLException e) {
            LOG.error(e.toString(), e);
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
                    Job job = MAPPER.readValue(jjson, Job.class);
                    if (id != job.id()) {
                        LOG.error("{} != {} in Database", id, job.id());
                        throw new AssertionError("id in JSON must be equal to the column");
                    }
                    String ajson = res.getString(2);
                    Application app = MAPPER.readValue(ajson, Application.class);

                    return Optional.of(new AppJobPair(Optional.of(app), job));
                }
                // No such application
            }
        } catch (SQLException e) {
            LOG.error(e.toString(), e);
        }
        return Optional.empty();
    }

    public Optional<Job> getJob(int id) throws JsonProcessingException, IOException {
        try (Connection conn = dataSource.getConnection(); //pool.getConnection();
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
            LOG.error(e.toString(), e);
        }
        return Optional.empty();
    }

    public Optional<Job> getJobFromTaskId(String taskId) throws JsonProcessingException, IOException {
        try (Connection conn = dataSource.getConnection(); //pool.getConnection();
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
            LOG.error(e.toString(), e);
        }
        return Optional.empty();
    }

    // Delete all jobs that has ID smaller than id
    public void deleteAllJob(int maxId) {
        try (Connection conn = dataSource.getConnection(); //pool.getConnection();
             PreparedStatement p = conn.prepareStatement("DELETE FROM jobs WHERE id < ?")) {
            conn.setAutoCommit(true);
            p.setInt(1, maxId);
            p.execute();
        } catch (SQLException e) {
            LOG.error(e.toString(), e);
        }
    }

    public void deleteOldJobs(int leeway) {
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            new Jobs(conn, MAPPER).collect(leeway);
        } catch (SQLException e) {
            LOG.error(e.toString(), e);
        }
    }

    public void setJobStarting(int id, Optional<String> maybeUrl, String taskId) throws IOException, SQLException, JobNotFoundException {
        updateJob(id, job -> {
            job.starting(taskId, maybeUrl, TimestampHelper.now());
            LOG.info("TaskId of id={}: {} / {}", id, taskId, job.taskId());
            return Optional.of(job);
        });
    }

    public void updateJob(int id, Function<Job, Optional<Job>> fun) throws IOException, SQLException, JobNotFoundException {
        try (Connection conn = dataSource.getConnection(); //pool.getConnection();
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
                        new Jobs(conn, MAPPER).updateJob(job);
                        conn.commit();
                        LOG.info("Job (id={}) status updated to {}", job.id(), job.state());
                    }
                } else {
                    throw new JobNotFoundException(id);
                }
            }
        }
    }

    public int countJobs() {
        try (Connection conn = dataSource.getConnection(); //pool.getConnection();
             PreparedStatement p = conn.prepareStatement("SELECT count(id) FROM jobs")) {
            conn.setAutoCommit(true);
            try (ResultSet set = p.executeQuery()) {
                if (set.next()) {
                    return set.getInt(1);
                }
            }
        } catch (SQLException e) {
            LOG.error(e.toString(), e);
        }
        return -1;
    }

    public int countRunning() {
        return countByState(Job.JobState.STARTED) + countByState(Job.JobState.STARTING);
    }

    public int countQueued() {
        return countByState(Job.JobState.QUEUED);
    }

    private int countByState(Job.JobState state) {
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
            LOG.error(e.toString(), e);
        }
        return -1;
    }

    public int getLatestJobId() {
        try (Connection conn = dataSource.getConnection(); //pool.getConnection();
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
            LOG.error(e.toString(), e);
        }
        return 0;
    }

    public List<Job> getRunning() {
        List<Job> jobs = new ArrayList<>(2);
        jobs.addAll(getByState(Job.JobState.STARTING));
        jobs.addAll(getByState(Job.JobState.STARTED));
        return jobs;
    }

    private List<Job> getByState(Job.JobState state) {
        List<Job> jobs = new ArrayList<>();
        try (Connection conn = dataSource.getConnection(); //pool.getConnection();
             PreparedStatement p = conn.prepareStatement("SELECT id, json FROM jobs WHERE state = ?")) {
            conn.setAutoCommit(true);
            p.setString(1, state.toString());
            try (ResultSet set = p.executeQuery()) {
                while (set.next()) {
                    String json = "";
                    try {
                        json = set.getString("json");
                        Job job = MAPPER.readValue(json, Job.class);
                        jobs.add(job);
                    } catch (IOException e) {
                        LOG.warn("Skipping job({}) due to exception", json, e);
                    }
                }
            }
        } catch (SQLException e) {
            LOG.error(e.toString(), e);
        }
        return jobs;
    }

    public boolean setFrameworkId(String value) {
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(true);
            LOG.info("setting new framework: {}", value);
            return new Property(conn).setFrameworkId(value);
        } catch (SQLException e) {
            LOG.error(e.toString(), e);
            return false;
        }
    }

    public Optional<String> getFrameworkId() {
        try (Connection conn = dataSource.getConnection()) {
            return new Property(conn).getFrameworkId();
        } catch (SQLException e) {
            LOG.error(e.toString(), e);
            return Optional.empty();
        }
    }

    public void deleteAllProperties() {
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            new Property(conn).deleteAll();
            conn.commit();
        } catch (SQLException e) {
            LOG.error(e.toString(), e);
        }
    }

    public void updateJobs(List<Job> list) {
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            Jobs jobs = new Jobs(conn, MAPPER);
            for (Job job : list) {
                jobs.updateJob(job);
            }
            conn.commit();
        } catch (JsonProcessingException | SQLException e) {
            LOG.error(e.toString(), e);
        }
    }

    public void retryJobs(List<Integer> ids) {
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            new Jobs(conn, MAPPER).doRetry(ids);
            conn.commit();
        } catch (SQLException e) {
            LOG.error(e.toString(), e);
        }
    }
}
