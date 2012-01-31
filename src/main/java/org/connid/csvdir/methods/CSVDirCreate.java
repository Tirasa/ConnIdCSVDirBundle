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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.connid.csvdir.CSVDirConfiguration;
import org.connid.csvdir.CSVDirConnection;
import org.identityconnectors.common.logging.Log;
import org.identityconnectors.common.security.GuardedString;
import org.identityconnectors.common.security.GuardedString.Accessor;
import org.identityconnectors.framework.common.exceptions.ConnectorException;
import org.identityconnectors.framework.common.objects.Attribute;
import org.identityconnectors.framework.common.objects.AttributeUtil;
import org.identityconnectors.framework.common.objects.Name;
import org.identityconnectors.framework.common.objects.OperationalAttributes;
import org.identityconnectors.framework.common.objects.Uid;
import org.identityconnectors.framework.spi.Connector;

public class CSVDirCreate extends CommonOperation {

    /**
     * Setup {@link Connector} based logging.
     */
    private static final Log LOG = Log.getLog(CSVDirCreate.class);

    private CSVDirConnection conn = null;

    private CSVDirConfiguration conf = null;

    private Set<Attribute> attrs = null;

    public CSVDirCreate(
            final CSVDirConfiguration conf,
            final Set<Attribute> set)
            throws SQLException, ClassNotFoundException {
        this.conf = conf;
        this.attrs = set;
        conn = CSVDirConnection.openConnection(conf);
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

        String uidString = "";
        Map<String, String> attributes = new HashMap<String, String>();

        if (AttributeUtil.getNameFromAttributes(attrs) == null) {
            throw new IllegalArgumentException("No Name attribute provided"
                    + "in the attributes");
        }

        for (Attribute attr : attrs) {
            if ((attr.getValue() != null) && (!attr.getValue().isEmpty())) {
                Object objValue = attr.getValue().get(0);

                if (attr.is(Name.NAME)) {
                    uidString = objValue.toString();
                } else {
                    final Set<String> name = new HashSet<String>();
                    final Set<String> value = new HashSet<String>();

                    if (attr.is(OperationalAttributes.PASSWORD_NAME)) {
                        ((GuardedString) objValue).access(new Accessor() {

                            @Override
                            public void access(char[] clearChars) {
                                name.add(conf.getPasswordColumnName());
                                value.add(new String(clearChars));
                            }
                        });
                    } else {
                        name.add(attr.getName());
                        value.add(objValue.toString());
                    }

                    attributes.put(
                            "\"" + name.iterator().next() + "\"",
                            "'" + value.iterator().next() + "'");
                }
            }
        }

        if (userExists(uidString, conn, conf)) {
            throw new ConnectorException("User Exists");
        }

        conn.insertAccount(attributes);

        LOG.ok("Creation commited");

        return new Uid(uidString);
    }
}
