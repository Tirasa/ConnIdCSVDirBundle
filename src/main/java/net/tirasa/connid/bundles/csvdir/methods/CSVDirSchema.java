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

import java.util.HashSet;
import java.util.Set;
import net.tirasa.connid.bundles.csvdir.CSVDirConfiguration;
import org.identityconnectors.common.logging.Log;
import org.identityconnectors.framework.common.exceptions.ConnectorException;
import org.identityconnectors.framework.common.objects.AttributeInfo;
import org.identityconnectors.framework.common.objects.AttributeInfoBuilder;
import org.identityconnectors.framework.common.objects.Name;
import org.identityconnectors.framework.common.objects.ObjectClass;
import org.identityconnectors.framework.common.objects.OperationalAttributes;
import org.identityconnectors.framework.common.objects.Schema;
import org.identityconnectors.framework.common.objects.SchemaBuilder;
import org.identityconnectors.framework.spi.Connector;

public class CSVDirSchema {

    /**
     * Setup {@link Connector} based logging.
     */
    private static final Log LOG = Log.getLog(CSVDirSchema.class);

    public static String OBJECTCLASS_DEFAULT_COLUMN_NAME = "__OC__";

    public static String OBJECTCLASS_DEFAULT_VALUE = ObjectClass.ACCOUNT.getObjectClassValue();

    private final CSVDirConfiguration conf;

    private final Class<? extends Connector> connectorClass;

    public CSVDirSchema(final Class<? extends Connector> connectorClass, final CSVDirConfiguration conf) {
        this.connectorClass = connectorClass;
        this.conf = conf;
    }

    public Schema execute() {
        try {
            return executeImpl();
        } catch (Exception e) {
            LOG.error(e, "error during updating");
            throw new ConnectorException(e);
        }
    }

    private Schema executeImpl() {
        final SchemaBuilder bld = new SchemaBuilder(connectorClass);
        final String[] keyColumns = conf.getKeyColumnNames();

        final Set<AttributeInfo> attrInfos = new HashSet<AttributeInfo>();

        for (final String fieldName : conf.getFields()) {

            if (!fieldName.equals(conf.getDeleteColumnName())) {
                final AttributeInfoBuilder abld = new AttributeInfoBuilder();

                if (fieldName.equalsIgnoreCase(conf.getPasswordColumnName())) {
                    abld.setName(OperationalAttributes.PASSWORD_NAME);
                }
                if (fieldName.equalsIgnoreCase(conf.getStatusColumn())) {
                    abld.setName(OperationalAttributes.ENABLE_NAME);
                } else if (keyColumns != null
                        && keyColumns.length == 1
                        && fieldName.equalsIgnoreCase(keyColumns[0])) {
                    abld.setName(Name.NAME);
                } else {
                    abld.setName(fieldName.trim());
                }

                abld.setCreateable(true);
                abld.setUpdateable(true);
                attrInfos.add(abld.build());
            }
        }

        if (keyColumns == null || keyColumns.length > 1) {
            final AttributeInfoBuilder abld = new AttributeInfoBuilder();
            abld.setName(Name.NAME);
            abld.setCreateable(true);
            abld.setUpdateable(true);
            attrInfos.add(abld.build());
        }

        // set it to specified object classes..
        for (String oc : conf.getObjectClass()) {
            bld.defineObjectClass(oc, attrInfos);
        }

        // return the new schema object..
        return bld.build();
    }
}
