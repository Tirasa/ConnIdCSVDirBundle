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

import java.sql.SQLException;
import net.tirasa.connid.bundles.csvdir.CSVDirConfiguration;
import net.tirasa.connid.bundles.csvdir.CSVDirConnection;
import org.identityconnectors.common.logging.Log;
import org.identityconnectors.framework.common.exceptions.ConnectorException;
import org.identityconnectors.framework.common.objects.Uid;
import org.identityconnectors.framework.spi.Connector;

public class CSVDirDelete extends CommonOperation {

    /**
     * Setup {@link Connector} based logging.
     */
    private static final Log LOG = Log.getLog(CSVDirDelete.class);

    private final CSVDirConnection conn;

    private final CSVDirConfiguration conf;

    private Uid uid = null;

    public CSVDirDelete(final CSVDirConfiguration conf, final Uid uid)
            throws ClassNotFoundException, SQLException {

        this.conf = conf;
        this.uid = uid;
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

    private void executeImpl() throws SQLException {
        if (!userExists(uid.getUidValue(), conn, conf)) {
            throw new ConnectorException("User does not exist");
        }
        conn.deleteAccount(uid);
        LOG.ok("Delete completed");
    }
}
