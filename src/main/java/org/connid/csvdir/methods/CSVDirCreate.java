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
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.connid.csvdir.CSVDirConfiguration;
import org.connid.csvdir.CSVDirConnection;
import org.connid.csvdir.database.QueryCreator;
import org.identityconnectors.common.logging.Log;
import org.identityconnectors.framework.common.exceptions.ConnectorException;
import org.identityconnectors.framework.common.objects.Attribute;
import org.identityconnectors.framework.common.objects.AttributeUtil;
import org.identityconnectors.framework.common.objects.Name;
import org.identityconnectors.framework.common.objects.Uid;

public class CSVDirCreate extends CommonOperation {

    /**
     * Setup {@link Connector} based logging.
     */
    private static final Log LOG = Log.getLog(CSVDirCreate.class);
    
    private CSVDirConnection connection = null;
    private CSVDirConfiguration configuration = null;
    private Set<Attribute> attrs = null;

    public CSVDirCreate(final CSVDirConfiguration configuration,
            final Set<Attribute> set)
            throws SQLException, ClassNotFoundException {
        this.configuration = configuration;
        this.attrs = set;
        connection = CSVDirConnection.openConnection(configuration);
    }

    public Uid execute() {
        try {
            return executeImpl();
        } catch (Exception e) {
            LOG.error(e, "error during creation");
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

    private Uid executeImpl() throws SQLException {
        
        String uidString = "";
        Map<String, String> map = new HashMap<String, String>();

        if (AttributeUtil.getNameFromAttributes(attrs) == null) {
            throw new IllegalArgumentException("No Name attribute provided"
                    + "in the attributes");
        }

        for (Attribute attr : attrs) {
            if (attr.is(Name.NAME)) {
                uidString = (String) attr.getValue().get(0);
            } else {
                List<Object> values = attr.getValue();
                if ((values != null) || (!values.isEmpty())) {
                    map.put("\"" + attr.getName() + "\"", "\'"
                            + (String) values.get(0) + "\'");
                }
            }
        }
        
        if (userExists(uidString, connection, configuration)) {
            throw new ConnectorException("User Exists");
        }
        
        connection.insertAccount(QueryCreator
                .insertQuery(map, configuration.getFields(),
                connection.createTableToWrite()));
        LOG.ok("Creation commited");
        return new Uid(uidString);
    }
}
