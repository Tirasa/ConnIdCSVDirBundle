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
package org.connid.csvdir.methods;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.regex.Pattern;
import org.connid.csvdir.CSVDirConfiguration;
import org.connid.csvdir.CSVDirConnection;
import org.identityconnectors.common.logging.Log;
import org.identityconnectors.framework.common.exceptions.ConnectorException;
import org.identityconnectors.framework.common.exceptions.ConnectorIOException;
import org.identityconnectors.framework.common.objects.AttributeBuilder;
import org.identityconnectors.framework.common.objects.ConnectorObjectBuilder;
import org.identityconnectors.framework.common.objects.ObjectClass;
import org.identityconnectors.framework.common.objects.OperationOptions;
import org.identityconnectors.framework.common.objects.SyncDeltaBuilder;
import org.identityconnectors.framework.common.objects.SyncDeltaType;
import org.identityconnectors.framework.common.objects.SyncResultsHandler;
import org.identityconnectors.framework.common.objects.SyncToken;
import org.identityconnectors.framework.common.objects.Uid;
import org.identityconnectors.framework.spi.Connector;

public class CSVDirSync extends CommonOperation {

    /**
     * Setup {@link Connector} based logging.
     */
    private static final Log LOG = Log.getLog(CSVDirExecuteQuery.class);

    private static long token = 0L;

    private CSVDirConfiguration configuration = null;

    private CSVDirConnection connection = null;

    private ObjectClass objectClass = null;

    private SyncToken syncToken = null;

    private SyncResultsHandler handler = null;

    private OperationOptions options = null;

    public CSVDirSync(final CSVDirConfiguration configuration,
            final ObjectClass objectClass,
            SyncToken syncToken,
            final SyncResultsHandler handler,
            final OperationOptions options)
            throws
            ClassNotFoundException, SQLException {
        this.configuration = configuration;
        this.objectClass = objectClass;
        this.syncToken = syncToken;
        this.handler = handler;
        this.options = options;
        connection = CSVDirConnection.openConnection(configuration);
    }

    public long execute() {
        try {
            return executeImpl();
        } catch (Exception e) {
            LOG.error(e, "error during updating");
            throw new ConnectorException(e);
        } finally {
            try {
                if (connection != null) {
                    connection.closeConnection();
                }
            } catch (SQLException e) {
                LOG.error(e, "Error closing connections");
            }
        }
    }

    private long executeImpl()
            throws SQLException {

        // check objectclass
        if (objectClass == null || (!objectClass.equals(ObjectClass.ACCOUNT))) {
            throw new IllegalArgumentException("Invalid objectclass");
        }

        // check objectclass
        if (handler == null) {
            throw new IllegalArgumentException("Invalid handler");
        }

        //check syncToken
        if ((syncToken == null) || syncToken.getValue() == null) {
            syncToken = new SyncToken(0);
        }

        CSVDirConnection connection = null;

        try {
            connection = CSVDirConnection.openConnection(configuration);

            buildSyncDelta(connection.modifiedCsvFiles(
                    Long.valueOf(syncToken.getValue().toString())), handler);

            token = connection.getFileSystem().getHighestTimeStamp();
        } catch (Exception e) {
            LOG.error(e, "error during syncronization");
            throw new ConnectorIOException(e);
        } finally {
            try {
                if (connection != null) {
                    connection.closeConnection();
                }
            } catch (SQLException e) {
                LOG.error(e, "Error closing connections");
            }
        }

        return token;
    }

    private void buildSyncDelta(final ResultSet rs,
            final SyncResultsHandler handler)
            throws SQLException {

        final ConnectorObjectBuilder bld = new ConnectorObjectBuilder();

        Boolean handled = Boolean.TRUE;

        while (rs.next() && handled) {
            for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
                String name = rs.getMetaData().getColumnName(i);
                String value = rs.getString(name);
                final String[] allValues = value.split(
                        Pattern.quote(configuration.getKeyseparator()), -1);

                if (name.equalsIgnoreCase(
                        configuration.getPasswordColumnName())) {
                    bld.addAttribute(AttributeBuilder.buildPassword(
                            value.toCharArray()));
                } else {
                    bld.addAttribute(name, Arrays.asList(allValues));
                }
            }

            final Uid uid = new Uid(
                    createUid(configuration.getKeyColumnNames(), rs,
                    configuration.getKeyseparator()));

            bld.setUid(uid);
            bld.setName(uid.getUidValue());

            final SyncDeltaBuilder syncDeltaBuilder = createSyncDelta(bld, uid);
            choseRightDeltaType(rs, syncDeltaBuilder);
            handler.handle(syncDeltaBuilder.build());
        }
    }

    private void choseRightDeltaType(final ResultSet rs,
            final SyncDeltaBuilder syncDeltaBuilder)
            throws SQLException {
        if (Boolean.valueOf(getValueFromColumnName(rs,
                configuration.getDeleteColumnName()))) {
            syncDeltaBuilder.setDeltaType(SyncDeltaType.DELETE);
        } else {
            syncDeltaBuilder.setDeltaType(SyncDeltaType.CREATE_OR_UPDATE);
        }
    }

    private SyncDeltaBuilder createSyncDelta(
            final ConnectorObjectBuilder connObjectBuilder, final Uid uid) {
        final SyncDeltaBuilder syncDeltaBuilder = new SyncDeltaBuilder();
        syncDeltaBuilder.setObject(connObjectBuilder.build());
        syncDeltaBuilder.setUid(uid);
        syncDeltaBuilder.setToken(getLatestSyncToken(ObjectClass.ACCOUNT));
        return syncDeltaBuilder;
    }

    private String getValueFromColumnName(final ResultSet rs,
            final String columnName)
            throws SQLException {
        return rs.getString(rs.findColumn(columnName));
    }

    private SyncToken getLatestSyncToken(final ObjectClass objectClass) {
        return new SyncToken(token);
    }
}
