/*
 * ====================
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 *
 * Copyright 2011 Tirasa. All rights reserved.
 *
 * The contents of this file are subject to the terms of the Common Development
 * and Distribution License("CDDL") (the "License").  You may not use this file
 * except in compliance with the License.
 *
 * You can obtain a copy of the License at
 * http://IdentityConnectors.dev.java.net/legal/license.txt
 * See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * When distributing the Covered Code, include this
 * CDDL Header Notice in each file
 * and include the License file at identityconnectors/legal/license.txt.
 * If applicable, add the following below this CDDL Header, with the fields
 * enclosed by brackets [] replaced by your own identifying information:
 * "Portions Copyrighted [year] [name of copyright owner]"
 * ====================
 */
package org.connid.csvdir;

import org.connid.csvdir.database.FileSystem;
import org.connid.csvdir.database.FileToDB;
import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.connid.csvdir.database.QueryCreator;
import org.connid.csvdir.utilities.Utilities;
import org.identityconnectors.common.logging.Log;
import org.identityconnectors.dbcommon.SQLParam;
import org.identityconnectors.dbcommon.SQLUtil;
import org.identityconnectors.framework.common.exceptions.ConnectorException;
import org.identityconnectors.framework.common.objects.Uid;

public class CSVDirConnection {

    /**
     * Setup logging for the {@link DatabaseConnection}.
     */
    private static final Log LOG = Log.getLog(CSVDirConnection.class);
    private String viewname = null;
    private String query = null;
    private String URL = "jdbc:hsqldb:file:";
    private List<String> tables = new ArrayList<String>();
    private Connection conn;
    private final CSVDirConfiguration configuration;
    private FileSystem fileSystem;
    private FileToDB fileToDB;

    private CSVDirConnection(final CSVDirConfiguration configuration)
            throws ClassNotFoundException, SQLException {

        this.configuration = configuration;
        URL += configuration.getSourcePath() + File.separator + "dbuser";
        fileSystem = new FileSystem(configuration);

        Class.forName("org.hsqldb.jdbcDriver");
        conn = DriverManager.getConnection(URL, "sa", "");
        conn.setAutoCommit(true);

        viewname = "USER_EX" + Utilities.randomNumber();
        query = "SELECT * FROM " + viewname;

        fileToDB = new FileToDB(configuration);
    }

    public static CSVDirConnection openConnection(
            final CSVDirConfiguration configuration)
            throws ClassNotFoundException, SQLException {

        return new CSVDirConnection(configuration);
    }

    public void closeConnection() throws SQLException {
        if (conn != null) {

            LOG.ok("Closing connection ...");

            dropTableAndViewIfExists();
            tables.clear();

            conn.close();
        }
    }

    public void deleteAccount(Uid uid) {
        File[] files = fileSystem.getAllCsvFiles();
        if (files.length == 0) {
            throw new ConnectorException("No file to delete");
        }

        String tableName = "";

        for (File file : files) {
            tableName = fileToDB.createDbForUpdate(conn, file);
            PreparedStatement stm = null;
            try {
                stm = conn.prepareStatement(
                        QueryCreator.deleteQuery(uid,
                        configuration.getKeyseparator(),
                        configuration.getKeyColumnNames(), tableName));
                LOG.ok("Execute update {0}", stm.toString());
                stm.executeUpdate();
            } catch (SQLException ex) {
                LOG.error(ex, "Error during sql query");
                throw new IllegalStateException(ex);
            } finally {
                try {
                    stm.close();
                } catch (SQLException ex) {
                    LOG.error(ex, "While closing sql statement");
                }
            }
        }
    }

    public int updateAccount(Map map, Uid uid) {
        int returnValue = 0;
        File[] files = fileSystem.getAllCsvFiles();
        if (files.length == 0) {
            throw new ConnectorException("No file to update");
        }

        String tableName = "";

        for (File file : files) {
            tableName = fileToDB.createDbForUpdate(conn, file);
            PreparedStatement stm = null;
            try {
                stm = conn.prepareStatement(
                        QueryCreator.updateQuery(map, uid,
                        configuration.getKeyseparator(),
                        configuration.getKeyColumnNames(), tableName));
                LOG.ok("Execute update {0}", stm.toString());
                returnValue = stm.executeUpdate();
            } catch (SQLException ex) {
                LOG.error(ex, "Error during sql query");
                throw new IllegalStateException(ex);
            } finally {
                try {
                    stm.close();
                } catch (SQLException ex) {
                    LOG.error(ex, "While closing sql statement");
                }
            }
        }
        return returnValue;
    }

    public String createTableToWrite() {
        String tableName = fileToDB.createDbForCreate(conn);
        tables.add(tableName);
        return tableName;
    }

    public int insertAccount(String query) {
        PreparedStatement stm = null;
        try {
            stm = conn.prepareStatement(query);
            LOG.ok("Execute update {0}", stm.toString());
            return stm.executeUpdate();
        } catch (SQLException ex) {
            LOG.error(ex, "Error during sql query");
            throw new IllegalStateException(ex);
        } finally {
            try {
                stm.close();
            } catch (SQLException ex) {
                LOG.error(ex, "While closing sql statement");
            }
        }
    }

    public final ResultSet modifiedCsvFiles(final long syncToken) {
        fileToDB.createDbForSync(fileSystem.getModifiedCsvFiles(syncToken),
                conn, tables, viewname);
        try {
            return doQuery(conn.prepareStatement(query));
        } catch (SQLException ex) {
            LOG.error(ex, "Error during sql query");
            throw new IllegalStateException(ex);
        }
    }

    public ResultSet allCsvFiles() {
        fileToDB.createDbForSync(fileSystem.getAllCsvFiles(), conn, tables,
                viewname);
        try {
            return doQuery(conn.prepareStatement(query));
        } catch (SQLException ex) {
            LOG.error(ex, "Error during sql query");
            throw new IllegalStateException(ex);
        }
    }

    public ResultSet allCsvFiles(String where, final List<SQLParam> params) {
        fileToDB.createDbForSync(fileSystem.getAllCsvFiles(), conn, tables,
                viewname);
        PreparedStatement stm = null;
        try {
            stm = conn.prepareStatement(
                    query + (where != null && !where.isEmpty()
                    ? " WHERE " + where : ""));

            SQLUtil.setParams(stm, params);

            return doQuery(stm);
        } catch (SQLException e) {
            LOG.error(e, "Error during sql query");
            throw new IllegalStateException(e);
        } finally {
            try {
                stm.close();
            } catch (SQLException ex) {
                LOG.error(ex, "While closing sql statement");
            }
        }
    }

    private ResultSet doQuery(final PreparedStatement stm)
            throws SQLException {
        LOG.ok("Execute query {0}", stm.toString());
        return stm.executeQuery();
    }

    private void dropTableAndViewIfExists()
            throws SQLException {
        LOG.ok("Drop view {0}", viewname);

        conn.createStatement().execute(
                "DROP VIEW " + viewname + " IF EXISTS CASCADE");

        for (String table : tables) {
            LOG.ok("Drop table {0}", table);

            conn.createStatement().execute(
                    "DROP TABLE " + table + " IF EXISTS CASCADE");
        }
    }

    public FileSystem getFileSystem() {
        return fileSystem;
    }

    public String getViewname() {
        return viewname;
    }
}
