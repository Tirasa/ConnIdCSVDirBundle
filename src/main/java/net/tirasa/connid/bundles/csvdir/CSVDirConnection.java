/**
 * Copyright (C) 2011 ConnId (connid-dev@googlegroups.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.tirasa.connid.bundles.csvdir;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import net.tirasa.connid.bundles.csvdir.database.FileSystem;
import net.tirasa.connid.bundles.csvdir.database.FileToDB;
import net.tirasa.connid.bundles.csvdir.database.QueryCreator;
import net.tirasa.connid.bundles.csvdir.utilities.Utilities;
import net.tirasa.connid.commons.db.DatabaseConnection;
import net.tirasa.connid.commons.db.SQLParam;
import net.tirasa.connid.commons.db.SQLUtil;
import org.hsqldb.jdbcDriver;
import org.identityconnectors.common.Pair;
import org.identityconnectors.common.StringUtil;
import org.identityconnectors.common.logging.Log;
import org.identityconnectors.framework.common.exceptions.ConnectorException;
import org.identityconnectors.framework.common.objects.ObjectClass;
import org.identityconnectors.framework.common.objects.Uid;

public class CSVDirConnection {

    /**
     * Setup logging for the {@link DatabaseConnection}.
     */
    private static final Log LOG = Log.getLog(CSVDirConnection.class);

    private static final String HSQLDB_JDBC_URL_PREFIX = "jdbc:hsqldb:file:";

    private static final String HSQLDB_DB_NAME = "csvdir_db";

    static {
        try {
            Class.forName(jdbcDriver.class.getName());
            System.setProperty("textdb.allow_full_path", "true");
        } catch (ClassNotFoundException e) {
            LOG.error(e, "Could not load " + jdbcDriver.class.getName());
        }
    }

    private final String viewname;

    private final String query;

    private final String jdbcUrl;

    private final Set<String> tables = new HashSet<String>();

    private final Connection conn;

    private final CSVDirConfiguration conf;

    private final FileSystem fileSystem;

    private final FileToDB fileToDB;

    private final QueryCreator queryCreator;

    private CSVDirConnection(final CSVDirConfiguration conf)
            throws ClassNotFoundException, SQLException {

        this.conf = conf;
        this.fileSystem = new FileSystem(conf);

        this.jdbcUrl = HSQLDB_JDBC_URL_PREFIX + conf.getSourcePath() + File.separator
                + HSQLDB_DB_NAME + ";shutdown=false";
        this.conn = DriverManager.getConnection(jdbcUrl, "sa", "");
        this.conn.setAutoCommit(true);

        this.viewname = "USER_EX" + Utilities.randomNumber();
        this.query = "SELECT * FROM " + viewname;

        this.fileToDB = new FileToDB(this);

        this.queryCreator = new QueryCreator(conf);
    }

    public static CSVDirConnection open(final CSVDirConfiguration conf) throws ClassNotFoundException, SQLException {
        return new CSVDirConnection(conf);
    }

    public void close() throws SQLException {
        if (this.conn != null) {
            LOG.ok("Closing connection ...");

            dropTableAndViewIfExists();

            SQLUtil.closeQuietly(conn);

            tables.clear();
        }
    }

    public int insertAccount(final ObjectClass oc, final Map<String, String> attributes) {
        final String tableName = fileToDB.createDbForCreate();

        tables.add(tableName);

        return execute(queryCreator.insertQuery(oc, attributes, tableName));
    }

    public int updateAccount(final ObjectClass oc, final Map<String, String> attrToBeReplaced, final Uid uid) {
        final File[] files = fileSystem.getAllCsvFiles();
        if (files.length == 0) {
            throw new ConnectorException("Empty table");
        }

        int returnValue = 0;

        for (File file : files) {
            final String tableName = fileToDB.createDbForUpdate(file);

            tables.add(tableName);

            returnValue += execute(queryCreator.updateQuery(
                    oc,
                    attrToBeReplaced,
                    uid,
                    conf.getKeyseparator(),
                    conf.getKeyColumnNames(),
                    tableName));
        }
        return returnValue;
    }

    public int deleteAccount(final ObjectClass oc, final Uid uid) {
        final File[] files = fileSystem.getAllCsvFiles();
        if (files.length == 0) {
            throw new ConnectorException("Empty table");
        }

        int returnValue = 0;

        for (File file : files) {
            final String tableName = fileToDB.createDbForUpdate(file);

            tables.add(tableName);

            returnValue += execute(queryCreator.deleteQuery(
                    oc,
                    uid,
                    conf.getKeyseparator(),
                    conf.getKeyColumnNames(),
                    tableName));

        }
        return returnValue;
    }

    private int execute(final String query) {
        PreparedStatement stmt = null;

        LOG.ok("About to execute {0}", query);
        try {
            stmt = conn.prepareStatement(query);
            return stmt.executeUpdate();
        } catch (SQLException e) {
            LOG.error(e, "Error during sql query");
            throw new IllegalStateException(e);
        } finally {
            SQLUtil.closeQuietly(stmt);
        }
    }

    public final Pair<Long, ResultSet> modifiedCsvFiles(
            final ObjectClass oc,
            final long syncToken) throws SQLException {
        final File[] csvFiles = fileSystem.getLastestChangedFiles(syncToken);
        final Long highestTimeStamp = fileSystem.getHighestTimeStamp(csvFiles);

        final List<String> tableNames = fileToDB.createDbForSync(csvFiles);
        tables.addAll(tableNames);

        final String ocColumnName = this.conf.getObjectClassColumn();

        String ocQuery = (StringUtil.isBlank(ocColumnName)
                ? query
                : new StringBuilder(query).append(" WHERE ").
                        append(ocColumnName).append("=").append("'").append(oc.getObjectClassValue()).append("'")).
                toString();
        return Pair.of(highestTimeStamp, doQuery(conn.prepareStatement(query)));
    }

    public ResultSet allCsvFiles(final ObjectClass oc) {
        final List<String> tableNames = fileToDB.createDbForSync(fileSystem.getAllCsvFiles());
        tables.addAll(tableNames);

        PreparedStatement stmt = null;
        try {
            final String ocColumnName = this.conf.getObjectClassColumn();

            String ocQuery = (StringUtil.isBlank(ocColumnName)
                    ? query
                    : new StringBuilder(query).append(" WHERE ").
                            append(ocColumnName).append("=").append("'").append(oc.getObjectClassValue()).append("'")).
                    toString();
            stmt = conn.prepareStatement(ocQuery);
            return doQuery(stmt);
        } catch (SQLException ex) {
            LOG.error(ex, "Error during sql query");
            throw new IllegalStateException(ex);
        } finally {
            SQLUtil.closeQuietly(stmt);
        }
    }

    public ResultSet allCsvFiles(final ObjectClass oc, final String where, final List<SQLParam> params) {
        final List<String> tableNames = fileToDB.createDbForSync(fileSystem.getAllCsvFiles());
        tables.addAll(tableNames);

        PreparedStatement stmt = null;
        try {
            final String ocColumnName = this.conf.getObjectClassColumn();

            String ocQuery = StringUtil.isBlank(ocColumnName)
                    ? query + (where != null && !where.isEmpty() ? " WHERE " + where : "")
                    : (new StringBuilder(query).append(" WHERE ").
                            append(ocColumnName).append("=").append("'").append(oc.getObjectClassValue()).append("'")).
                            toString() + (where != null && !where.isEmpty() ? " AND " + where : "");

            stmt = conn.prepareStatement(ocQuery);

            SQLUtil.setParams(stmt, params);

            return doQuery(stmt);
        } catch (SQLException e) {
            LOG.error(e, "Error during sql query");
            throw new IllegalStateException(e);
        } finally {
            SQLUtil.closeQuietly(stmt);
        }
    }

    private ResultSet doQuery(final PreparedStatement stm) throws SQLException {
        LOG.ok("Execute query {0}", stm.toString());
        return stm.executeQuery();
    }

    private void dropTableAndViewIfExists() throws SQLException {
        LOG.ok("Drop view {0}", viewname);

        Statement stmt = null;
        try {
            stmt = conn.createStatement();
            stmt.execute("DROP VIEW " + viewname + " IF EXISTS CASCADE");
        } finally {
            SQLUtil.closeQuietly(stmt);
        }

        for (String table : tables) {
            LOG.ok("Drop table {0}", table);

            try {
                stmt = conn.createStatement();
                stmt.execute("DROP TABLE " + table + " IF EXISTS CASCADE");
            } finally {
                SQLUtil.closeQuietly(stmt);
            }
        }
    }

    public FileSystem getFileSystem() {
        return fileSystem;
    }

    public String getViewname() {
        return viewname;
    }

    public Connection getConn() {
        return conn;
    }

    public CSVDirConfiguration getConf() {
        return conf;
    }

    public Set<String> getTables() {
        return tables;
    }
}
