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
 * https://connid.googlecode.com/svn/base/trunk/legal/license.txt
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
package org.connid.bundles.csvdir.methods;

import java.sql.ResultSet;
import java.sql.SQLException;
import org.connid.bundles.csvdir.CSVDirConfiguration;
import org.connid.bundles.csvdir.CSVDirConnection;
import org.identityconnectors.common.logging.Log;
import org.identityconnectors.framework.common.exceptions.ConnectorException;
import org.identityconnectors.framework.common.exceptions.ConnectorIOException;
import org.identityconnectors.framework.common.objects.ConnectorObject;
import org.identityconnectors.framework.common.objects.ConnectorObjectBuilder;
import org.identityconnectors.framework.common.objects.ObjectClass;
import org.identityconnectors.framework.common.objects.OperationOptions;
import org.identityconnectors.framework.common.objects.SyncDeltaBuilder;
import org.identityconnectors.framework.common.objects.SyncDeltaType;
import org.identityconnectors.framework.common.objects.SyncResultsHandler;
import org.identityconnectors.framework.common.objects.SyncToken;
import org.identityconnectors.framework.spi.Connector;

public class CSVDirSync extends CommonOperation {

    /**
     * Setup {@link Connector} based logging.
     */
    private static final Log LOG = Log.getLog(CSVDirExecuteQuery.class);

    private static long token = 0L;

    private CSVDirConfiguration conf = null;

    private CSVDirConnection connection = null;

    private ObjectClass objectClass = null;

    private SyncToken syncToken = null;

    private SyncResultsHandler handler = null;

    private OperationOptions options = null;

    public CSVDirSync(final CSVDirConfiguration conf,
            final ObjectClass objectClass,
            SyncToken syncToken,
            final SyncResultsHandler handler,
            final OperationOptions options)
            throws
            ClassNotFoundException, SQLException {
        this.conf = conf;
        this.objectClass = objectClass;
        this.syncToken = syncToken;
        this.handler = handler;
        this.options = options;
        connection = CSVDirConnection.openConnection(conf);
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

        CSVDirConnection conn = null;

        try {
            conn = CSVDirConnection.openConnection(conf);

            buildSyncDelta(conn.modifiedCsvFiles(
                    Long.valueOf(syncToken.getValue().toString())), handler);

            token = conn.getFileSystem().getHighestTimeStamp();
        } catch (Exception e) {
            LOG.error(e, "error during syncronization");
            throw new ConnectorIOException(e);
        } finally {
            try {
                if (conn != null) {
                    conn.closeConnection();
                }
            } catch (SQLException e) {
                LOG.error(e, "Error closing connections");
            }
        }

        return token;
    }

    private void buildSyncDelta(
            final ResultSet rs,
            final SyncResultsHandler handler)
            throws SQLException {

        boolean handled = true;

        while (rs.next() && handled) {
            final ConnectorObjectBuilder bld = buildConnectorObject(conf, rs);

            final SyncDeltaBuilder syncDeltaBuilder = createSyncDelta(bld);
            choseRightDeltaType(rs, syncDeltaBuilder);
            handled = handler.handle(syncDeltaBuilder.build());
        }
    }

    private void choseRightDeltaType(final ResultSet rs,
            final SyncDeltaBuilder syncDeltaBuilder)
            throws SQLException {
        if (Boolean.valueOf(getValueFromColumnName(rs,
                conf.getDeleteColumnName()))) {
            syncDeltaBuilder.setDeltaType(SyncDeltaType.DELETE);
        } else {
            syncDeltaBuilder.setDeltaType(SyncDeltaType.CREATE_OR_UPDATE);
        }
    }

    private SyncDeltaBuilder createSyncDelta(
            final ConnectorObjectBuilder connObjectBuilder) {
        final SyncDeltaBuilder syncDeltaBuilder = new SyncDeltaBuilder();

        ConnectorObject object = connObjectBuilder.build();
        syncDeltaBuilder.setObject(object);
        syncDeltaBuilder.setUid(object.getUid());
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
