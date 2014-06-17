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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.connid.bundles.csvdir.CSVDirConfiguration;
import org.connid.bundles.csvdir.CSVDirConnection;
import org.connid.bundles.db.common.FilterWhereBuilder;
import org.connid.bundles.db.common.SQLParam;
import org.identityconnectors.common.logging.Log;
import org.identityconnectors.framework.common.exceptions.ConnectorException;
import org.identityconnectors.framework.common.exceptions.ConnectorIOException;
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

    private final CSVDirConfiguration conf;

    private final CSVDirConnection conn;

    private final ObjectClass oclass;

    private final FilterWhereBuilder where;

    private final ResultsHandler handler;

    private final OperationOptions options;

    public CSVDirExecuteQuery(final CSVDirConfiguration configuration,
            final ObjectClass oclass,
            final FilterWhereBuilder where,
            final ResultsHandler handler,
            final OperationOptions options)
            throws ClassNotFoundException, SQLException {

        this.conf = configuration;
        this.oclass = oclass;
        this.where = where;
        this.handler = handler;
        this.options = options;
        this.conn = CSVDirConnection.openConnection(configuration);
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

        final String whereClause = where == null ? null : where.getWhereClause();
        LOG.ok("Where Clause {0}", whereClause);

        final List<SQLParam> params = where == null ? null : where.getParams();
        LOG.ok("Where Params {0}", params);

        ResultSet resultSet = null;
        try {
            resultSet = conn.allCsvFiles(whereClause, params);

            boolean handled = true;

            while (resultSet.next() && handled) {
                if (!Boolean.valueOf(resultSet.getString(resultSet.findColumn(conf.getDeleteColumnName())))) {
                    // create the connector object..
                    handled = handler.handle(buildConnectorObject(conf, resultSet));
                }
            }
        } catch (Exception e) {
            LOG.error(e, "Search query failed");
            throw new ConnectorIOException(e);
        } finally {
            try {
                if (resultSet != null) {
                    resultSet.close();
                }
            } catch (SQLException e) {
                LOG.error(e, "Error closing result set");
            }
        }
        LOG.ok("Query Account commited");
    }

    private Set<String> resolveColumnNamesToGet() {
        final Set<String> attributesToGet = new HashSet<String>();
        attributesToGet.add(Uid.NAME);

        String[] attributes = null;
        if (options == null || options.getAttributesToGet() == null) {
            attributes = conf.getFields();
        } else {
            attributes = options.getAttributesToGet();
        }

        attributesToGet.addAll(Arrays.asList(attributes));
        return attributesToGet;
    }
}
