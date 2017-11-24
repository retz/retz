package io.github.retz.db.migration;

import org.apache.tomcat.jdbc.pool.DataSource;
import org.flywaydb.core.Flyway;

public class DBMigration {
    public static void migrate(DataSource dataSource) {
        Flyway flyway = new Flyway();
        flyway.setDataSource(dataSource);
        flyway.setBaselineOnMigrate(true);
        flyway.migrate();
    }
}
