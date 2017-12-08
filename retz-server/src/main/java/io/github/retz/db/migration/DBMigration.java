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
