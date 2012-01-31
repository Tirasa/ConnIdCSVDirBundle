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
 */package org.connid.csvdir.database;

import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.connid.csvdir.CSVDirConfiguration;
import org.connid.csvdir.utilities.Utilities;
import org.identityconnectors.common.logging.Log;
import org.identityconnectors.framework.spi.Connector;

public class FileToDB {

    private CSVDirConfiguration conf = null;

    private FileSystem fileSystem = null;

    public FileToDB(final CSVDirConfiguration conf) {
        this.conf = conf;
        fileSystem = new FileSystem(conf);
    }

    /**
     * Setup {@link Connector} based logging.
     */
    private static final Log LOG = Log.getLog(FileToDB.class);

    public String createDbForCreate(final Connection conn) {
        File file = fileSystem.getLastModifiedCsvFile();
        if (file == null) {
            file = new File("createFile" + Utilities.randomNumber() + ".csv");
        }
        return bindFileTable(file, conn);
    }

    public String createDbForUpdate(
            final Connection conn, final File file) {
        return bindFileTable(file, conn);
    }

    public List<String> createDbForSync(
            final File[] fileToProcess,
            final Connection conn,
            final String viewname) {
        return bindFileTables(fileToProcess, conn, viewname);
    }

    private StringBuilder createTableHeader() {
        final StringBuilder tableHeader = new StringBuilder();
        for (String field : conf.getFields()) {
            tableHeader.append(field.trim()).append(" ").append("VARCHAR(255), ");
        }
        tableHeader.append("PRIMARY KEY (");

        final String[] keys = conf.getKeyColumnNames();
        for (int i = 0; i < keys.length; i++) {
            if (i > 0) {
                tableHeader.append(",");
            }
            tableHeader.append(keys[i]);
        }
        tableHeader.append(")");

        return tableHeader;
    }

    private List<String> bindFileTables(
            final File[] files,
            final Connection conn,
            final String viewname) {

        final StringBuilder view = new StringBuilder();

        final List<String> tables = new ArrayList<String>();

        for (File file : files) {
            final String tableName = bindFileTable(file, conn);

            if (tableName != null) {
                tables.add(tableName);

                if (view.length() != 0) {
                    view.append(" UNION ");
                }
                view.append("SELECT * FROM ").append(tableName);
            }
        }

        if (view.length() != 0) {
            try {
                view.insert(0, "CREATE VIEW " + viewname + " AS ");

                LOG.ok("Execute: {0}", view.toString());
                conn.createStatement().execute(view.toString());
            } catch (SQLException e) {
                LOG.error(e, "While creating view {0}", viewname);
            }
        } else {
            try {
                LOG.ok("Execute: CREATE TEXT TABLE NOENTRIES");

                final StringBuilder tableHeader = createTableHeader();
                final StringBuilder createTable = new StringBuilder();

                createTable.delete(0, createTable.length());
                createTable.append("CREATE TEXT TABLE NOENTRIES");
                createTable.append(" (").append(tableHeader).append(") ");
                conn.createStatement().execute(createTable.toString());

                tables.add("NOENTRIES");

                conn.createStatement().execute(
                        "CREATE VIEW " + viewname + " AS SELECT * FROM NOENTRIES");
            } catch (SQLException e) {
                LOG.error(e, "While creating table NOENTRIES");
            }
        }

        return tables;
    }

    private String bindFileTable(
            final File file,
            final Connection conn) {

        if (LOG.isOk()) {
            LOG.ok("File to load {0}", file.getAbsolutePath());
        }

        try {
            final StringBuilder tableHeader = createTableHeader();

            final String tableName = "CSV_TABLE" + Utilities.randomNumber();

            final StringBuilder createTable = new StringBuilder();
            final StringBuilder linkTable = new StringBuilder();

            createTable.delete(0, createTable.length());
            createTable.append("CREATE TEXT TABLE ").append(tableName);
            createTable.append(" (").append(tableHeader).append(") ");
            linkTable.delete(0, createTable.length());
            linkTable.append("SET TABLE ").append(tableName).
                    append(" SOURCE ").
                    append("\"").
                    append(File.separator).append(file.getName()).
                    append(";ignore_first=").
                    append(conf.getIgnoreHeader()).
                    append(";all_quoted=").
                    append(conf.getQuotationRequired()).
                    append(";fs=").
                    append(conf.getFieldDelimiter()).
                    append(";lvs=").
                    append(conf.getTextQualifier() == '"'
                    ? "\\quote" : conf.getTextQualifier()).
                    append(";encoding=").
                    append(conf.getEncoding()).
                    append("\"");

            LOG.ok("Execute: {0}", createTable.toString());
            conn.createStatement().execute(createTable.toString());

            LOG.ok("Execute: {0}", linkTable.toString());
            conn.createStatement().execute(linkTable.toString());

            return tableName;
        } catch (SQLException e) {
            LOG.error(e, "While creating text table");
            return null;
        }
    }
}
