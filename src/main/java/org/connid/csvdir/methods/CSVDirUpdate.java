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

import java.sql.SQLException;
import java.util.Set;
import org.connid.csvdir.CSVDirConfiguration;
import org.connid.csvdir.CSVDirConnection;
import org.identityconnectors.common.StringUtil;
import org.identityconnectors.common.logging.Log;
import org.identityconnectors.framework.common.exceptions.ConnectorException;
import org.identityconnectors.framework.common.objects.Attribute;
import org.identityconnectors.framework.common.objects.Uid;
import org.identityconnectors.framework.spi.Connector;

public class CSVDirUpdate extends CommonOperation {

    /**
     * Setup {@link Connector} based logging.
     */
    private static final Log LOG = Log.getLog(CSVDirUpdate.class);

    private CSVDirConnection connection = null;

    private CSVDirConfiguration conf = null;

    private Uid uid = null;

    private Set<Attribute> attrs = null;

    public CSVDirUpdate(final CSVDirConfiguration conf,
            final Uid uid, final Set<Attribute> set)
            throws
            ClassNotFoundException, SQLException {
        this.conf = conf;
        this.uid = uid;
        this.attrs = set;
        connection = CSVDirConnection.openConnection(conf);
    }

    public Uid execute() {
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

    private Uid executeImpl()
            throws SQLException {

        if (uid == null || StringUtil.isBlank(uid.getUidValue())) {
            throw new IllegalArgumentException(
                    "No Name attribute provided in the attributes");
        }

        if (!userExists(uid.getUidValue(), connection, conf)) {
            throw new ConnectorException("User doesn't exist");
        }

        connection.updateAccount(getAttributeMap(conf, attrs), uid);

        LOG.ok("Creation commited");
        return uid;
    }
}
