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
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import org.connid.csvdir.CSVDirConfiguration;
import org.connid.csvdir.utilities.Utilities;
import org.identityconnectors.common.logging.Log;

public class FileToDB {
    
    private CSVDirConfiguration configuration = null;
    private FileSystem fileSystem = null;
    private File fileToUpdate = null;

    public FileToDB(final CSVDirConfiguration configuration) {
        this.configuration = configuration;
        fileSystem = new FileSystem(configuration);
    }

    /**
     * Setup {@link Connector} based logging.
     */
    private static final Log LOG = Log.getLog(FileToDB.class);
    
    public String createDbForCreate(Connection conn) {
        final StringBuilder tableHeader = createTableHeader();
        String tableName = "";
        try {
            tableName = processFiles(tableHeader, conn);
        } catch (Exception e) {
            LOG.error(e, "While creating database");
        }
        return tableName;
    }

    private String processFiles(
            final StringBuilder tableHeader, final Connection conn)
            throws IOException, SQLException {

        final StringBuilder createTable = new StringBuilder();
        final StringBuilder linkTable = new StringBuilder();

        File[] files = fileSystem.
                getModifiedCsvFiles(fileSystem.getHighestTimeStamp());
        if (files.length == 0) {
            fileToUpdate = new File("createFile"
                    + Utilities.randomNumber() + ".csv");
        } else {
            fileToUpdate = files[0];
        }
        
        //TODO: controllare
//        if (configuration.getQuotationRequired()) {
//            final PrintWriter wrt = new PrintWriter(new BufferedWriter(
//				new FileWriter(fileToUpdate, true)));
//            for (String field : configuration.getFields()) {
//                wrt.println(field);
//            }
//            wrt.close();
//        }

        LOG.ok("File to load {0}", fileToUpdate.getAbsolutePath());

        String tableName = "CSV_TABLE" + Utilities.randomNumber();

        try {
            createTable.delete(0, createTable.length());
            createTable.append("CREATE TEXT TABLE ").append(tableName);
            createTable.append(" (").append(tableHeader).append(") ");
            linkTable.delete(0, createTable.length());
            linkTable.append("SET TABLE ").append(tableName).
                    append(" SOURCE ").
                    append("\"").
                    append(File.separator).append(fileToUpdate.getName()).
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
            
            LOG.ok("Execute: {0}", linkTable.toString());
            conn.createStatement().execute(linkTable.toString());
        } catch (SQLException e) {
            LOG.error(e, "While creating text table");
        }
        return tableName;
    }
    
    private PrintWriter writeOutFileData(final File file)
            throws FileNotFoundException, UnsupportedEncodingException {
        return new PrintWriter(
                new OutputStreamWriter(new FileOutputStream(file),
                "UTF-8"));
    }
    
    public String createDbForUpdate(Connection conn, File file) {
        final StringBuilder tableHeader = createTableHeader();
        String tableName = "";
        try {
            tableName = processFilesForUpdate(tableHeader, conn, file);
        } catch (Exception e) {
            LOG.error(e, "While creating database");
        }
        return tableName;
    }
    
    private String processFilesForUpdate(
            final StringBuilder tableHeader, final Connection conn, final File file)
            throws IOException, SQLException {

        final StringBuilder createTable = new StringBuilder();
        final StringBuilder linkTable = new StringBuilder();
        
        //TODO: controllare
//        if (configuration.getQuotationRequired()) {
//            final PrintWriter wrt = new PrintWriter(new BufferedWriter(
//				new FileWriter(fileToUpdate, true)));
//            for (String field : configuration.getFields()) {
//                wrt.println(field);
//            }
//            wrt.close();
//        }

        LOG.ok("File to load {0}", file.getAbsolutePath());

        String tableName = "CSV_TABLE" + Utilities.randomNumber();

        try {
            createTable.delete(0, createTable.length());
            createTable.append("CREATE TEXT TABLE ").append(tableName);
            createTable.append(" (").append(tableHeader).append(") ");
            linkTable.delete(0, createTable.length());
            linkTable.append("SET TABLE ").append(tableName).
                    append(" SOURCE ").
                    append("\"").
                    append(File.separator).append(file.getName()).
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
            
            LOG.ok("Execute: {0}", linkTable.toString());
            conn.createStatement().execute(linkTable.toString());
        } catch (SQLException e) {
            LOG.error(e, "While creating text table");
        }
        return tableName;
    }
    
    public void createDbForSync(final File[] fileToProcess,
            final Connection conn,
            final List<String> tables, final String viewname) {
        StringBuilder tableHeader = createTableHeader();
        try {
            processAllFiles(fileToProcess, tableHeader, conn, tables, viewname);
        } catch (Exception e) {
            LOG.error(e, "While creating database");
        }
    }
    
    private StringBuilder createTableHeader() {
        final StringBuilder tableHeader = new StringBuilder();
        for (String field : configuration.getFields()) {
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
        return tableHeader;
    }

    private void processAllFiles(
            final File[] fileToProcess,
            final StringBuilder tableHeader, final Connection conn,
            final List<String> tables, final String viewname)
            throws IOException, SQLException {

        String tableName;
        final StringBuilder createTable = new StringBuilder();
        final StringBuilder linkTable = new StringBuilder();
        final StringBuilder view = new StringBuilder();

        for (File csvFile : fileToProcess) {
            LOG.ok("File to load {0}", csvFile.getAbsolutePath());

            tableName = "CSV_TABLE" + Utilities.randomNumber();

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
}
