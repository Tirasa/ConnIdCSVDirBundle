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
package net.tirasa.connid.bundles.csvdir.methods;

import java.sql.ResultSet;
import java.sql.SQLException;
import net.tirasa.connid.bundles.csvdir.CSVDirConfiguration;
import net.tirasa.connid.bundles.csvdir.CSVDirConnection;
import org.identityconnectors.common.Pair;
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

    public void execute() {
        try {
            executeImpl();
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

    private void executeImpl()
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
            final Pair<Long, ResultSet> modified = conn.modifiedCsvFiles(Long.valueOf(syncToken.getValue().toString()));
            buildSyncDelta(modified.getValue(), modified.getKey(), handler);
        } catch (NumberFormatException e) {
            LOG.error(e, "error during syncronization");
            throw new ConnectorIOException(e);
        } catch (SQLException e) {
            LOG.error(e, "error during syncronization");
            throw new ConnectorIOException(e);
        }
    }

    private void buildSyncDelta(
            final ResultSet resultSet,
            final Long token,
            final SyncResultsHandler handler)
            throws SQLException {

        boolean handled = true;

        try {
            while (resultSet.next() && handled) {
                final ConnectorObject connObject = buildConnectorObject(conf, resultSet);

                final SyncDeltaBuilder syncDeltaBuilder = createSyncDelta(connObject, token);
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

    private SyncDeltaBuilder createSyncDelta(final ConnectorObject connObject, final Long token) {
        final SyncDeltaBuilder syncDeltaBuilder = new SyncDeltaBuilder();

        syncDeltaBuilder.setObject(connObject);
        syncDeltaBuilder.setUid(connObject.getUid());
        syncDeltaBuilder.setToken(new SyncToken(token));
        return syncDeltaBuilder;
    }

    private String getValueFromColumnName(final ResultSet rs, final String columnName) throws SQLException {
        return rs.getString(rs.findColumn(columnName));
    }
}
