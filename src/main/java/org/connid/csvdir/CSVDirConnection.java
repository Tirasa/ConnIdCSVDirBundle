/*
 * ====================
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 * 
 * Copyright 2008-2009 Sun Microsystems, Inc. All rights reserved.     
 * 
 * The contents of this file are subject to the terms of the Common Development 
 * and Distribution License("CDDL") (the "License").  You may not use this file 
 * except in compliance with the License.
 * 
 * You can obtain a copy of the License at 
 * http://IdentityConnectors.dev.java.net/legal/license.txt
 * See the License for the specific language governing permissions and limitations 
 * under the License. 
 * 
 * When distributing the Covered Code, include this CDDL Header Notice in each file
 * and include the License file at identityconnectors/legal/license.txt.
 * If applicable, add the following below this CDDL Header, with the fields 
 * enclosed by brackets [] replaced by your own identifying information: 
 * "Portions Copyrighted [year] [name of copyright owner]"
 * ====================
 */
package org.connid.csvdir;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.identityconnectors.common.logging.Log;
import org.identityconnectors.dbcommon.DatabaseConnection;
import org.identityconnectors.dbcommon.SQLParam;
import org.identityconnectors.dbcommon.SQLUtil;

/**
 *
 * @author fabio
 */
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

    private CSVDirConnection(final CSVDirConfiguration configuration)
            throws SQLException {
        this.configuration = configuration;
        URL += configuration.getSourcePath() + File.separator + "dbuser";
        fileSystem = new FileSystem(configuration);

        conn = DriverManager.getConnection(URL, "sa", "");
        conn.setAutoCommit(true);

        viewname = "USER_EX" + (int) (Math.random() * 100000);
        query = "SELECT * FROM " + viewname;
    }

    public static CSVDirConnection openConnection(
            final CSVDirConfiguration configuration) throws SQLException {

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

    public final ResultSet modifiedCsvFiles(final long syncToken) {
        createDb(fileSystem.getModifiedCsvFiles(syncToken));
        try {
            return doQuery(conn.prepareStatement(query));
        } catch (SQLException ex) {
            LOG.error("Error during sql query", ex);
            throw new IllegalStateException(ex);
        }
    }

    public ResultSet allCsvFiles(String where, final List<SQLParam> params) {
        createDb(fileSystem.getAllCsvFiles());

        try {
            final PreparedStatement stm = conn.prepareStatement(
                    query + (where != null && !where.isEmpty()
                    ? " WHERE " + where : ""));

            SQLUtil.setParams(stm, params);

            return doQuery(stm);
        } catch (SQLException e) {
            LOG.error(e, "Error during sql query");
            throw new IllegalStateException(e);
        }
    }

    private ResultSet doQuery(final PreparedStatement stm) throws SQLException {
        LOG.ok("Execute query {0}", stm.toString());
        return stm.executeQuery();
    }

    private void createDb(final File[] fileToProcess) {
        final String[] fields = configuration.getFields();
        final StringBuilder tableHeader = new StringBuilder();

        for (String field : fields) {
            tableHeader.append(field.trim()).append(" ").
                    append("VARCHAR(255), ");
        }

        tableHeader.append("PRIMARY KEY (");
        final String[] keys = configuration.getKeyColumnNames();
        for (int i = 0; i < keys.length; i++) {
            if (i > 0) {
                tableHeader.append(",");
            }
            tableHeader.append(keys[i]);
        }
        tableHeader.append(")");

        try {

            processAllFiles(fileToProcess, tableHeader);

        } catch (Exception e) {
            LOG.error(e, "While creating database");
        }
    }

    private void processAllFiles(
            final File[] fileToProcess,
            final StringBuilder tableHeader) throws IOException, SQLException {

        String tableName;
        final StringBuilder createTable = new StringBuilder();
        final StringBuilder linkTable = new StringBuilder();
        final StringBuilder view = new StringBuilder();

        for (File csvFile : fileToProcess) {
            LOG.ok("File to load {0}", csvFile.getAbsolutePath());

            tableName = "CSV_TABLE" + (int) (Math.random() * 100000);

            try {
                createTable.delete(0, createTable.length());
                createTable.append("CREATE TEXT TABLE ").append(tableName);
                createTable.append(" (").append(tableHeader).append(") ");
                linkTable.delete(0, createTable.length());
                linkTable.append("SET TABLE ").append(tableName).
                        append(" SOURCE ").
                        append("\"").
                        append(File.separator).append(csvFile.getName()).
                        append(";ignore_first=").
                        append(configuration.getIgnoreHeader()).
                        append(";all_quoted=").
                        append(configuration.getQuotationRequired()).
                        append(";fs=").
                        append(configuration.getFieldDelimiter()).
                        append(";lvs=").
                        append(configuration.getTextQualifier() == '"'
                        ? "\\quote" : configuration.getTextQualifier()).
                        append(";encoding=").
                        append(configuration.getEncoding()).
                        append("\"");

                LOG.ok("Execute: {0}", createTable.toString());
                conn.createStatement().execute(createTable.toString());

                tables.add(tableName);

                LOG.ok("Execute: {0}", linkTable.toString());
                conn.createStatement().execute(linkTable.toString());

                if (view.length() != 0) {
                    view.append(" UNION ");
                }
                view.append("SELECT * FROM ").append(tableName);
            } catch (SQLException e) {
                LOG.error(e, "While creating text table");
            }
        }

        LOG.ok("Create view {0}", viewname);

        if (view.length() != 0) {
            view.insert(0, "CREATE VIEW " + viewname + " AS ");

            LOG.ok("Execute: {0}", view.toString());
            conn.createStatement().execute(view.toString());
        } else {
            LOG.ok("Execute: CREATE TEXT TABLE NOENTRIES");

            createTable.delete(0, createTable.length());
            createTable.append("CREATE TEXT TABLE NOENTRIES");
            createTable.append(" (").append(tableHeader).append(") ");
            conn.createStatement().execute(createTable.toString());

            tables.add("NOENTRIES");

            conn.createStatement().execute(
                    "CREATE VIEW " + viewname + " AS SELECT * FROM NOENTRIES");
        }
    }

    private void dropTableAndViewIfExists() throws SQLException {
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
