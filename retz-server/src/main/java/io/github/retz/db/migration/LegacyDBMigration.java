package io.github.retz.db.migration;

import io.github.retz.db.Database;
import org.apache.tomcat.jdbc.pool.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.util.Objects;

import static java.nio.charset.StandardCharsets.UTF_8;

public class LegacyDBMigration {

    private final static Logger LOG = LoggerFactory.getLogger(LegacyDBMigration.class);

    public static void migrate(DataSource dataSource) throws SQLException, IOException {
        try (Connection conn = dataSource.getConnection()) { //pool.getConnection()) {
            conn.setAutoCommit(false);
            DatabaseMetaData meta = conn.getMetaData();

            if (!meta.supportsTransactionIsolationLevel(Connection.TRANSACTION_SERIALIZABLE)) {
                LOG.error("Current database ({}) does not support required isolation level ({})",
                        dataSource.getUrl(), Connection.TRANSACTION_SERIALIZABLE);
                throw new RuntimeException("Current database does not support serializable");
            }
            maybeCreateTables(conn);
            conn.commit();
        }
    }

    private static void maybeCreateTables(Connection conn) throws SQLException, IOException {
        LOG.info("Checking database schema of {} ...", conn.getMetaData().getURL());

        if (allTableExists(conn)) {
            LOG.info("All four table exists.");
        } else {
            LOG.info("No table exists: creating....");

            InputStream ddl = LegacyDBMigration.class.getClassLoader().getResourceAsStream("db/migration/legacy/retz-ddl.sql");
            String createString = org.apache.commons.io.IOUtils.toString(ddl, UTF_8);
            //System.err.println(createString);
            try (Statement statement = conn.createStatement()) {
                statement.execute(createString);
            }
        }
    }

    public static boolean allTableExists() throws IOException {
        try (Connection conn = Database.getDataSource().getConnection()) {
            return allTableExists(conn);
        } catch (SQLException e) {
            throw new IOException("Database.allTableExists() failed", e);
        }
    }

    private static boolean allTableExists(Connection conn) throws SQLException, IOException {
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
            LOG.error("user:{}, applicaion:{}, job:{}, prop:{}", userTableExists, applicationTableExists, jobTableExists, propTableExists);
            throw new AssertionError("Database is partially ready: quitting");
        }
    }

    private static boolean tableExists(DatabaseMetaData meta, String schemaPattern, String tableName) throws SQLException {
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
}
