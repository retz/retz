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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;
import java.util.Properties;

// DAO that provides system property: 'properties' table in retz-ddl.sql
// Could be different from general DAO definition :P
public class Property {
    private static final Logger LOG = LoggerFactory.getLogger(Property.class);

    private final Connection conn;

    public Property(Connection c) {
        conn = c;
    }

    public boolean setFrameworkId(String value) {
        return setProperty("FrameworkID", value);
    }

    public Optional<String> getFrameworkId() {
        return getProperty("FrameworkID");
    }

    private boolean setProperty(String key, String value) {
        try (PreparedStatement p = conn.prepareStatement("INSERT INTO properties(key, value, epoch) VALUES (?, ?, ?)")) {
            conn.setAutoCommit(false);
            p.setString(1, key);
            p.setString(2, value);
            p.setInt(3, 0);
            p.execute();
            return true;
        } catch (SQLException e) {
            LOG.error("setting {}={} into properties => {}: trying update...", key, value, e.toString());
            return updateProperty(key, value);
        }
    }

    private boolean updateProperty(String key, String value) {
        try (PreparedStatement p = conn.prepareStatement("UPDATE properties SET value=?, epoch=epoch+1 WHERE key=?")) {
            conn.setAutoCommit(false);
            p.setString(3, key);
            p.setString(1, value);
            p.execute();
            return true;
        } catch (SQLException e) {
            LOG.error(e.toString(), e);
            return false;
        }
    }

    private Optional<String> getProperty(String key) {
        try (PreparedStatement p = conn.prepareStatement("SELECT key, value, epoch FROM properties WHERE key=?")) {
            p.setString(1, key);
            try (ResultSet res = p.executeQuery()) {
                if (res.next()) {
                    return Optional.of(res.getString("value"));
                }
                return Optional.empty();
            }
        } catch (SQLException e) {
            LOG.error(e.toString(), e);
            return Optional.empty();
        }
    }

    public Properties getAllProperties() {
        Properties props = new Properties();
        try (PreparedStatement p = conn.prepareStatement("SELECT key, value, epoch FROM properties")) {
            try (ResultSet res = p.executeQuery()) {
                while (res.next()) {
                    props.setProperty(res.getString("key"), res.getString("value"));
                }
            }
        } catch (SQLException e) {
            LOG.error(e.toString(), e);
        }
        return props;
    }

    public void deleteAll() {
        try (PreparedStatement p = conn.prepareStatement("SELECT key, value, epoch FROM properties")) {

        } catch (SQLException e) {
            LOG.error(e.toString(), e);
        }
    }
}
