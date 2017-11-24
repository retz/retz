package io.github.retz.db.migration;

import javax.sql.DataSource;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.FlywayException;

import java.io.IOException;
import java.sql.SQLException;

public class DBMigration {
    private Flyway flyway = new Flyway();

    public DBMigration(DataSource dataSource) {
        flyway.setDataSource(dataSource);
        flyway.setBaselineOnMigrate(true);
    }

    public void migrate() throws SQLException, IOException {
        try {
            LegacyDBMigration.migrate((org.apache.tomcat.jdbc.pool.DataSource) flyway.getDataSource());
            flyway.migrate();
        } catch (FlywayException e) {
            throw new IOException(e);
        }
    }

    public void clean() throws IOException {
        try {
            flyway.clean();
        } catch (FlywayException e) {
            throw new IOException(e);
        }
    }

    public boolean isFinished() throws IOException {
        try {
            return LegacyDBMigration.allTableExists() && flyway.info().pending().length < 1;
        } catch (FlywayException e) {
            throw new IOException(e);
        }
    }
}
