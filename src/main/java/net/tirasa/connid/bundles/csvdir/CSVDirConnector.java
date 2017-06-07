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
package net.tirasa.connid.bundles.csvdir;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import net.tirasa.connid.bundles.csvdir.database.FileSystem;
import net.tirasa.connid.bundles.csvdir.methods.CSVDirCreate;
import net.tirasa.connid.bundles.csvdir.methods.CSVDirDelete;
import net.tirasa.connid.bundles.csvdir.methods.CSVDirExecuteQuery;
import net.tirasa.connid.bundles.csvdir.methods.CSVDirFilterTranslator;
import net.tirasa.connid.bundles.csvdir.methods.CSVDirSchema;
import net.tirasa.connid.bundles.csvdir.methods.CSVDirSync;
import net.tirasa.connid.bundles.csvdir.methods.CSVDirTest;
import net.tirasa.connid.bundles.csvdir.methods.CSVDirUpdate;
import net.tirasa.connid.commons.db.FilterWhereBuilder;
import org.identityconnectors.common.logging.Log;
import org.identityconnectors.common.security.GuardedString;
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

@ConnectorClass(configurationClass = CSVDirConfiguration.class, displayNameKey = "CSVDir")
public class CSVDirConnector implements Connector,
        SearchOp<FilterWhereBuilder>, SchemaOp, SyncOp, CreateOp, UpdateOp, DeleteOp, AuthenticateOp, TestOp {

    /**
     *
     * Setup {@link Connector} based logging.
     *
     */
    private static final Log LOG = Log.getLog(CSVDirConnector.class);

    /**
     *
     * Configuration information passed back to the {@link Connector} by the method
     *
     * {@link Connector#init(Configuration)}.
     *
     */
    private CSVDirConfiguration configuration;

    @Override
    public final Configuration getConfiguration() {
        return configuration;
    }

    /**
     * @param cfg Saves the configuration for use in later calls.
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
            new CSVDirExecuteQuery(configuration, oclass, where, handler, options).execute();
        } catch (ClassNotFoundException e) {
            throw new ConnectorIOException(e);
        } catch (SQLException e) {
            throw new ConnectorIOException(e);
        }
    }

    @Override
    public final void sync(
            final ObjectClass objectClass,
            final SyncToken syncToken,
            final SyncResultsHandler handler,
            final OperationOptions options) {

        try {
            new CSVDirSync(configuration, objectClass, syncToken, handler, options).execute();
        } catch (ClassNotFoundException e) {
            throw new ConnectorIOException(e);
        } catch (SQLException ex) {
            throw new ConnectorIOException(ex);
        }
    }

    @Override
    public final SyncToken getLatestSyncToken(final ObjectClass objectClass) {
        return new SyncToken(new FileSystem(configuration).getHighestTimeStamp(0L));
    }

    @Override
    public void dispose() {
        // no actions
    }

    @Override
    public final Uid create(final ObjectClass objectClass, final Set<Attribute> set,
            final OperationOptions options) {

        try {
            return new CSVDirCreate(configuration, set).execute();
        } catch (ClassNotFoundException e) {
            throw new ConnectorIOException(e);
        } catch (SQLException e) {
            throw new ConnectorIOException(e);
        }
    }

    @Override
    public final Uid update(final ObjectClass objectClass, final Uid uid,
            final Set<Attribute> attrs, final OperationOptions options) {

        try {
            return new CSVDirUpdate(configuration, uid, attrs).execute();
        } catch (ClassNotFoundException e) {
            throw new ConnectorIOException(e);
        } catch (SQLException e) {
            throw new ConnectorIOException(e);
        }
    }

    @Override
    public final void delete(final ObjectClass objectClass, final Uid uid,
            final OperationOptions options) {

        try {
            new CSVDirDelete(configuration, uid).execute();
        } catch (ClassNotFoundException e) {
            throw new ConnectorIOException(e);
        } catch (SQLException e) {
            throw new ConnectorIOException(e);
        }
    }

    @Override

    public Uid authenticate(
            final ObjectClass objectClass,
            final String username,
            final GuardedString password,
            final OperationOptions options) {

        final List<Uid> res = new ArrayList<Uid>();

        final CSVDirFilterTranslator translator = new CSVDirFilterTranslator(this, objectClass, options);

        password.access(new GuardedString.Accessor() {

            @Override
            public void access(final char[] clearChars) {

                final Filter uid = FilterBuilder.equalTo(AttributeBuilder.build(Uid.NAME, username));

                final Filter pwd = FilterBuilder.equalTo(
                        AttributeBuilder.build(configuration.getPasswordColumnName(), new String(clearChars)));

                final Filter filter = FilterBuilder.and(uid, pwd);

                final List<Uid> results = new ArrayList<Uid>();

                final ResultsHandler handler = new ResultsHandler() {

                    @Override
                    public boolean handle(final ConnectorObject obj) {
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
        } catch (Exception e) {
            LOG.error("Test failed", e);
        }
    }
}
