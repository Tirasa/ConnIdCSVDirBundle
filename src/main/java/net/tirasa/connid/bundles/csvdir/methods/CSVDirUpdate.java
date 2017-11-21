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
import java.util.Set;
import net.tirasa.connid.bundles.csvdir.CSVDirConfiguration;
import net.tirasa.connid.bundles.csvdir.CSVDirConnection;
import org.identityconnectors.common.StringUtil;
import org.identityconnectors.common.logging.Log;
import org.identityconnectors.framework.common.exceptions.ConnectorException;
import org.identityconnectors.framework.common.objects.Attribute;
import org.identityconnectors.framework.common.objects.Name;
import org.identityconnectors.framework.common.objects.Uid;
import org.identityconnectors.framework.spi.Connector;

public class CSVDirUpdate extends CommonOperation {

    /**
     * Setup {@link Connector} based logging.
     */
    private static final Log LOG = Log.getLog(CSVDirUpdate.class);

    private final CSVDirConnection conn;

    private final CSVDirConfiguration conf;

    private final Uid uid;

    private Set<Attribute> attrs = null;

    public CSVDirUpdate(final CSVDirConfiguration conf,
            final Uid uid, final Set<Attribute> set)
            throws ClassNotFoundException, SQLException {

        this.conf = conf;
        this.uid = uid;
        this.attrs = set;
        this.conn = CSVDirConnection.open(conf);
    }

    public Uid execute() {
        try {
            return executeImpl();
        } catch (Exception e) {
            LOG.error(e, "error during updating");
            throw new ConnectorException(e);
        } finally {
            try {
                conn.close();
            } catch (SQLException e) {
                LOG.error(e, "Error closing connections");
            }
        }
    }

    private Uid executeImpl() throws SQLException {
        if (uid == null || StringUtil.isBlank(uid.getUidValue())) {
            throw new IllegalArgumentException("No Name attribute provided in the attributes");
        }

        if (!userExists(uid.getUidValue(), conn, conf)) {
            throw new ConnectorException("User doesn't exist");
        }

        conn.updateAccount(getAttributeMap(conf, attrs, new Name(uid.getUidValue())), uid);

        LOG.ok("Update commited");
        return uid;
    }
}
