/**
 * ====================
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 *
 * Copyright 2008-2009 Sun Microsystems, Inc. All rights reserved.
 * Copyright 2011-2013 Tirasa. All rights reserved.
 *
 * The contents of this file are subject to the terms of the Common Development
 * and Distribution License("CDDL") (the "License"). You may not use this file
 * except in compliance with the License.
 *
 * You can obtain a copy of the License at https://oss.oracle.com/licenses/CDDL
 * See the License for the specific language governing permissions and limitations
 * under the License.
 *
 * When distributing the Covered Code, include this CDDL Header Notice in each file
 * and include the License file at https://oss.oracle.com/licenses/CDDL.
 * If applicable, add the following below this CDDL Header, with the fields
 * enclosed by brackets [] replaced by your own identifying information:
 * "Portions Copyrighted [year] [name of copyright owner]"
 * ====================
 */
package org.connid.bundles.csvdir.methods;

import java.sql.SQLException;
import java.util.Set;
import org.connid.bundles.csvdir.CSVDirConfiguration;
import org.connid.bundles.csvdir.CSVDirConnection;
import org.identityconnectors.common.StringUtil;
import org.identityconnectors.common.logging.Log;
import org.identityconnectors.framework.common.exceptions.ConnectorException;
import org.identityconnectors.framework.common.objects.Attribute;
import org.identityconnectors.framework.common.objects.AttributeUtil;
import org.identityconnectors.framework.common.objects.Name;
import org.identityconnectors.framework.common.objects.Uid;
import org.identityconnectors.framework.spi.Connector;

public class CSVDirCreate extends CommonOperation {

    /**
     * Setup {@link Connector} based logging.
     */
    private static final Log LOG = Log.getLog(CSVDirCreate.class);

    private final CSVDirConnection conn;

    private final CSVDirConfiguration conf;

    private final Set<Attribute> attrs;

    public CSVDirCreate(
            final CSVDirConfiguration conf,
            final Set<Attribute> attrs)
            throws SQLException, ClassNotFoundException {

        this.conf = conf;
        this.attrs = attrs;
        this.conn = CSVDirConnection.openConnection(conf);
    }

    public Uid execute() {
        try {
            return executeImpl();
        } catch (Exception e) {
            LOG.error(e, "error during creation");
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

    private Uid executeImpl()
            throws SQLException {

        final Name name = AttributeUtil.getNameFromAttributes(attrs);

        if (name == null || StringUtil.isBlank(name.getNameValue())) {
            throw new IllegalArgumentException(
                    "No Name attribute provided in the attributes");
        }

        if (userExists(name.getNameValue(), conn, conf)) {
            throw new ConnectorException("User Exists");
        }

        conn.insertAccount(getAttributeMap(conf, attrs, name));

        LOG.ok("Creation commited");

        return new Uid(name.getNameValue());
    }
}
