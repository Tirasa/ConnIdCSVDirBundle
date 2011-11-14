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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import org.connid.csvdir.CSVDirConfiguration;
import org.connid.csvdir.CSVDirConnection;
import org.identityconnectors.common.logging.Log;
import org.identityconnectors.dbcommon.FilterWhereBuilder;
import org.identityconnectors.dbcommon.SQLParam;
import org.identityconnectors.framework.common.exceptions.ConnectorException;
import org.identityconnectors.framework.common.exceptions.ConnectorIOException;
import org.identityconnectors.framework.common.objects.AttributeBuilder;
import org.identityconnectors.framework.common.objects.ConnectorObjectBuilder;
import org.identityconnectors.framework.common.objects.ObjectClass;
import org.identityconnectors.framework.common.objects.OperationOptions;
import org.identityconnectors.framework.common.objects.ResultsHandler;
import org.identityconnectors.framework.common.objects.Uid;
import org.identityconnectors.framework.spi.Connector;

public class CSVDirExecuteQuery extends CommonOperation {

    /**
     * Setup {@link Connector} based logging.
     */
    private static final Log LOG = Log.getLog(CSVDirExecuteQuery.class);

    private CSVDirConfiguration configuration = null;

    private CSVDirConnection connection = null;

    private ObjectClass oclass = null;

    private FilterWhereBuilder where = null;

    private ResultsHandler handler = null;

    private OperationOptions options = null;

    public CSVDirExecuteQuery(final CSVDirConfiguration configuration,
            final ObjectClass oclass,
            final FilterWhereBuilder where,
            final ResultsHandler handler,
            final OperationOptions options)
            throws
            ClassNotFoundException, SQLException {
        this.configuration = configuration;
        this.oclass = oclass;
        this.where = where;
        this.handler = handler;
        this.options = options;
        connection = CSVDirConnection.openConnection(configuration);
    }

    public void execute() {
        try {
            executeImpl();
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

    private void executeImpl()
            throws SQLException {
        LOG.info("check the ObjectClass and result handler");

        // Contract tests
        if (oclass == null || (!oclass.equals(ObjectClass.ACCOUNT))) {
            throw new IllegalArgumentException("Object class required");
        }

        if (handler == null) {
            throw new IllegalArgumentException("Result handler required");
        }

        LOG.ok("The ObjectClass and result handler is ok");

        final Set<String> columnNamesToGet = resolveColumnNamesToGet();

        LOG.ok("Column Names {0} To Get", columnNamesToGet);

        final String whereClause =
                where != null ? where.getWhereClause() : null;

        LOG.ok("Where Clause {0}", whereClause);

        final List<SQLParam> params =
                where != null ? where.getParams() : null;

        LOG.ok("Where Params {0}", params);

        ResultSet rs = null;

        try {
            rs = connection.allCsvFiles(whereClause, params);

            final ConnectorObjectBuilder bld = new ConnectorObjectBuilder();
            String name = "";
            String value = "";

            Boolean handled = Boolean.TRUE;

            while (rs.next() && handled) {
                for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
                    name = rs.getMetaData().getColumnName(i);
                    value = rs.getString(name);
                    final String[] allValues = value == null
                            ? new String[]{}
                            : value.split(
                            Pattern.quote(configuration.getKeyseparator()), -1);

                    if (name.equalsIgnoreCase(
                            configuration.getPasswordColumnName())) {
                        bld.addAttribute(AttributeBuilder.buildPassword(
                                value.toCharArray()));
                    } else {
                        bld.addAttribute(name, Arrays.asList(allValues));
                    }
                }

                final String uid = createUid(
                        configuration.getKeyColumnNames(),
                        rs,
                        configuration.getKeyseparator());

                bld.setUid(uid);
                bld.setName(uid);

                // create the connector object..
                handled = handler.handle(bld.build());
            }
        } catch (Exception e) {
            LOG.error(e, "Search query failed");
            throw new ConnectorIOException(e);
        } finally {
            try {
                if (rs != null) {
                    rs.close();
                }

                if (connection != null) {
                    connection.closeConnection();
                }
            } catch (SQLException e) {
                LOG.error(e, "Error closing connections");
            }
        }
        LOG.ok("Query Account commited");
    }

    private Set<String> resolveColumnNamesToGet() {

        final Set<String> attributesToGet = new HashSet<String>();
        attributesToGet.add(Uid.NAME);

        String[] attributes = null;

        if (options != null && options.getAttributesToGet() != null) {
            attributes = options.getAttributesToGet();
        } else {
            attributes = configuration.getFields();
        }

        attributesToGet.addAll(Arrays.asList(attributes));
        return attributesToGet;
    }
}
