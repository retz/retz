package io.github.retz.db.migration;

import javax.sql.DataSource;
import org.flywaydb.core.Flyway;

import java.io.IOException;
import java.sql.SQLException;

public class DBMigration {
    private Flyway flyway = new Flyway();

    public DBMigration(DataSource dataSource) {
        flyway.setDataSource(dataSource);
        flyway.setBaselineOnMigrate(true);
    }

    public void migrate() throws SQLException, IOException {
        LegacyDBMigration.migrate((org.apache.tomcat.jdbc.pool.DataSource) flyway.getDataSource());
        flyway.migrate();
    }

    public void clean() {
        flyway.clean();
    }

    public boolean isFinished() throws IOException {
        return LegacyDBMigration.allTableExists() && flyway.info().pending().length < 1;
    }
}
