/* 
 * ====================
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 * 
 * Copyright 2011 ConnId. All rights reserved.
 * 
 * The contents of this file are subject to the terms of the Common Development
 * and Distribution License("CDDL") (the "License").  You may not use this file
 * except in compliance with the License.
 * 
 * You can obtain a copy of the License at
 * http://opensource.org/licenses/cddl1.php
 * See the License for the specific language governing permissions and limitations
 * under the License.
 * 
 * When distributing the Covered Code, include this CDDL Header Notice in each file
 * and include the License file at http://opensource.org/licenses/cddl1.php.
 * If applicable, add the following below this CDDL Header, with the fields
 * enclosed by brackets [] replaced by your own identifying information:
 * "Portions Copyrighted [year] [name of copyright owner]"
 * ====================
 */
package net.tirasa.connid.bundles.csvdir.methods;

import java.sql.ResultSet;
import java.sql.SQLException;
import net.tirasa.connid.bundles.csvdir.CSVDirConfiguration;
import net.tirasa.connid.bundles.csvdir.CSVDirConnection;
import org.identityconnectors.common.StringUtil;
import org.identityconnectors.common.logging.Log;
import org.identityconnectors.framework.common.exceptions.ConnectorException;
import org.identityconnectors.framework.common.exceptions.ConnectorIOException;
import org.identityconnectors.framework.common.objects.ConnectorObject;
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

    private final CSVDirConfiguration conf;

    private final CSVDirConnection conn;

    private final ObjectClass objectClass;

    private SyncToken syncToken;

    private final SyncResultsHandler handler;

    private final OperationOptions options;

    public CSVDirSync(final CSVDirConfiguration conf,
            final ObjectClass objectClass,
            final SyncToken syncToken,
            final SyncResultsHandler handler,
            final OperationOptions options)
            throws ClassNotFoundException, SQLException {

        this.conf = conf;
        this.objectClass = objectClass;
        this.syncToken = syncToken;
        this.handler = handler;
        this.options = options;
        this.conn = CSVDirConnection.openConnection(conf);
    }

    public long execute() {
        try {
            return executeImpl();
        } catch (Exception e) {
            LOG.error(e, "error during updating");
            throw new ConnectorException(e);
        } finally {
            try {
                if (conn != null) {
                    conn.closeConnection();
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

        try {
            buildSyncDelta(conn.modifiedCsvFiles(Long.valueOf(syncToken.getValue().toString())), handler);

            token = conn.getFileSystem().getHighestTimeStamp();
        } catch (NumberFormatException e) {
            LOG.error(e, "error during syncronization");
            throw new ConnectorIOException(e);
        } catch (SQLException e) {
            LOG.error(e, "error during syncronization");
            throw new ConnectorIOException(e);
        }

        return token;
    }

    private void buildSyncDelta(
            final ResultSet resultSet,
            final SyncResultsHandler handler)
            throws SQLException {

        boolean handled = true;

        try {
            while (resultSet.next() && handled) {
                final ConnectorObject connObject = buildConnectorObject(conf, resultSet);

                final SyncDeltaBuilder syncDeltaBuilder = createSyncDelta(connObject);
                choseRightDeltaType(resultSet, syncDeltaBuilder);
                handled = handler.handle(syncDeltaBuilder.build());
            }
        } finally {
            if (resultSet != null) {
                resultSet.close();
            }
        }
    }

    private void choseRightDeltaType(final ResultSet rs, final SyncDeltaBuilder syncDeltaBuilder)
            throws SQLException {

        if (StringUtil.isNotBlank(conf.getDeleteColumnName())
                && Boolean.valueOf(getValueFromColumnName(rs, conf.getDeleteColumnName()))) {
            syncDeltaBuilder.setDeltaType(SyncDeltaType.DELETE);
        } else {
            syncDeltaBuilder.setDeltaType(SyncDeltaType.CREATE_OR_UPDATE);
        }
    }

    private SyncDeltaBuilder createSyncDelta(final ConnectorObject connObject) {
        final SyncDeltaBuilder syncDeltaBuilder = new SyncDeltaBuilder();

        syncDeltaBuilder.setObject(connObject);
        syncDeltaBuilder.setUid(connObject.getUid());
        syncDeltaBuilder.setToken(getLatestSyncToken());
        return syncDeltaBuilder;
    }

    private String getValueFromColumnName(final ResultSet rs, final String columnName) throws SQLException {
        return rs.getString(rs.findColumn(columnName));
    }

    private SyncToken getLatestSyncToken() {
        return new SyncToken(token);
    }
}
