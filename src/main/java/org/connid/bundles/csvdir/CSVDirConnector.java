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
 * https://connid.googlecode.com/svn/base/trunk/legal/license.txt
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
package org.connid.bundles.csvdir;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.connid.bundles.csvdir.methods.CSVDirCreate;
import org.connid.bundles.csvdir.methods.CSVDirDelete;
import org.connid.bundles.csvdir.methods.CSVDirExecuteQuery;
import org.connid.bundles.csvdir.methods.CSVDirFilterTranslator;
import org.connid.bundles.csvdir.methods.CSVDirSchema;
import org.connid.bundles.csvdir.methods.CSVDirSync;
import org.connid.bundles.csvdir.methods.CSVDirTest;
import org.connid.bundles.csvdir.methods.CSVDirUpdate;
import org.identityconnectors.common.logging.Log;
import org.identityconnectors.common.security.GuardedString;
import org.identityconnectors.dbcommon.FilterWhereBuilder;
import org.identityconnectors.framework.common.exceptions.ConnectorIOException;
import org.identityconnectors.framework.common.exceptions.InvalidCredentialException;
import org.identityconnectors.framework.common.objects.Attribute;
import org.identityconnectors.framework.common.objects.AttributeBuilder;
import org.identityconnectors.framework.common.objects.ConnectorObject;
import org.identityconnectors.framework.common.objects.ObjectClass;
import org.identityconnectors.framework.common.objects.OperationOptions;
import org.identityconnectors.framework.common.objects.OperationOptionsBuilder;
import org.identityconnectors.framework.common.objects.ResultsHandler;
import org.identityconnectors.framework.common.objects.Schema;
import org.identityconnectors.framework.common.objects.SyncResultsHandler;
import org.identityconnectors.framework.common.objects.SyncToken;
import org.identityconnectors.framework.common.objects.Uid;
import org.identityconnectors.framework.common.objects.filter.Filter;
import org.identityconnectors.framework.common.objects.filter.FilterBuilder;
import org.identityconnectors.framework.common.objects.filter.FilterTranslator;
import org.identityconnectors.framework.spi.Configuration;
import org.identityconnectors.framework.spi.Connector;
import org.identityconnectors.framework.spi.ConnectorClass;
import org.identityconnectors.framework.spi.operations.AuthenticateOp;
import org.identityconnectors.framework.spi.operations.CreateOp;
import org.identityconnectors.framework.spi.operations.DeleteOp;
import org.identityconnectors.framework.spi.operations.SchemaOp;
import org.identityconnectors.framework.spi.operations.SearchOp;
import org.identityconnectors.framework.spi.operations.SyncOp;
import org.identityconnectors.framework.spi.operations.TestOp;
import org.identityconnectors.framework.spi.operations.UpdateOp;

/**
 * Only implements search since this connector is only used to do sync.
 */
@ConnectorClass(configurationClass = CSVDirConfiguration.class,
displayNameKey = "FlatFile")
public class CSVDirConnector implements
        Connector, SearchOp<FilterWhereBuilder>, SchemaOp, SyncOp, CreateOp,
        UpdateOp, DeleteOp, AuthenticateOp, TestOp {

    /**
     * Setup {@link Connector} based logging.
     */
    private static final Log LOG = Log.getLog(CSVDirConnector.class);

    /**
     * Configuration information passed back to the {@link Connector} by the method
     * {@link Connector#init(Configuration)}.
     */
    private CSVDirConfiguration configuration;

    private long token = 0L;

    @Override
    public final Configuration getConfiguration() {
        return configuration;
    }

    /**
     * @param cfg Saves the configuration for use in later calls.
     * @see org.identityconnectors.framework.Connector#init( org.identityconnectors.framework.Configuration)
     */
    @Override
    public final void init(final Configuration cfg) {
        configuration = (CSVDirConfiguration) cfg;
    }

    @Override
    public final Schema schema() {
        return new CSVDirSchema(getClass(), configuration).execute();
    }

    @Override
    public final FilterTranslator<FilterWhereBuilder> createFilterTranslator(
            final ObjectClass oclass, final OperationOptions options) {
        if (oclass == null || (!oclass.equals(ObjectClass.ACCOUNT))) {
            throw new IllegalArgumentException("Invalid objectclass");
        }
        return new CSVDirFilterTranslator(this, oclass, options);
    }

    @Override
    public final void executeQuery(
            final ObjectClass oclass,
            final FilterWhereBuilder where,
            final ResultsHandler handler,
            final OperationOptions options) {
        try {
            new CSVDirExecuteQuery(
                    configuration, oclass, where, handler, options).execute();
        } catch (ClassNotFoundException ex) {
            LOG.error(ex, "error during execute query");
            throw new ConnectorIOException(ex);
        } catch (SQLException ex) {
            LOG.error(ex, "error during execute query");
            throw new ConnectorIOException(ex);
        }
    }

    @Override
    public final void sync(
            final ObjectClass objectClass,
            final SyncToken syncToken,
            final SyncResultsHandler handler,
            final OperationOptions options) {

        try {
            token = new CSVDirSync(
                    configuration, objectClass, syncToken, handler, options).
                    execute();
        } catch (ClassNotFoundException ex) {
            LOG.error(ex, "error during creation");
            throw new ConnectorIOException(ex);
        } catch (SQLException ex) {
            LOG.error(ex, "error during creation");
            throw new ConnectorIOException(ex);
        }
    }

    @Override
    public final SyncToken getLatestSyncToken(final ObjectClass objectClass) {
        return new SyncToken(token);
    }

    @Override
    public void dispose() {
        // no actions
    }

    @Override
    public final Uid create(final ObjectClass oc, final Set<Attribute> set,
            final OperationOptions oo) {
        try {
            return new CSVDirCreate(configuration, set).execute();
        } catch (ClassNotFoundException ex) {
            LOG.error(ex, "error during creation");
            throw new ConnectorIOException(ex);
        } catch (SQLException ex) {
            LOG.error(ex, "error during creation");
            throw new ConnectorIOException(ex);
        }
    }

    @Override
    public final Uid update(final ObjectClass oc, final Uid uid,
            final Set<Attribute> set, final OperationOptions oo) {
        try {
            return new CSVDirUpdate(configuration, uid, set).execute();
        } catch (ClassNotFoundException ex) {
            LOG.error(ex, "error during update operation");
            throw new ConnectorIOException(ex);
        } catch (SQLException ex) {
            LOG.error(ex, "error during update operation");
            throw new ConnectorIOException(ex);
        }
    }

    @Override
    public final void delete(final ObjectClass oc, final Uid uid,
            final OperationOptions oo) {
        try {
            new CSVDirDelete(configuration, uid).execute();
        } catch (ClassNotFoundException ex) {
            LOG.error(ex, "error during delete operation");
            throw new ConnectorIOException(ex);
        } catch (SQLException ex) {
            LOG.error(ex, "error delete operation");
            throw new ConnectorIOException(ex);
        }
    }

    @Override
    public Uid authenticate(
            final ObjectClass objectClass,
            final String username,
            final GuardedString password,
            final OperationOptions options) {

        final List<Uid> res = new ArrayList<Uid>();

        final CSVDirFilterTranslator translator =
                new CSVDirFilterTranslator(this, objectClass, options);

        password.access(new GuardedString.Accessor() {

            @Override
            public void access(final char[] clearChars) {
                final Filter uid = FilterBuilder.equalTo(
                        AttributeBuilder.build(Uid.NAME, username));

                final Filter pwd = FilterBuilder.equalTo(
                        AttributeBuilder.build(
                        configuration.getPasswordColumnName(),
                        new String(clearChars)));

                final Filter filter = FilterBuilder.and(uid, pwd);

                final List<Uid> results = new ArrayList<Uid>();

                final ResultsHandler handler = new ResultsHandler() {

                    @Override
                    public boolean handle(ConnectorObject obj) {
                        if (obj != null && obj.getUid() != null) {
                            results.add(obj.getUid());
                            return true;
                        } else {
                            return false;
                        }
                    }
                };

                final OperationOptionsBuilder op = new OperationOptionsBuilder();
                op.setAttributesToGet();

                executeQuery(
                        objectClass,
                        translator.translate(filter).get(0),
                        handler,
                        op.build());

                if (results.isEmpty()) {
                    throw new InvalidCredentialException("User not found");
                }

                res.addAll(results);
            }
        });

        return res.get(0);
    }

    @Override
    public final void test() {
        LOG.info("Connection test");
        try {
            new CSVDirTest(configuration).test();
        } catch (ClassNotFoundException ex) {
            LOG.error("Test failed", ex);
        } catch (SQLException ex) {
            LOG.error("Test failed", ex);
        }
    }
}
