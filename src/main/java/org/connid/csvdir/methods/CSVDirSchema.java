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

import java.util.HashSet;
import java.util.Set;
import org.connid.csvdir.CSVDirConfiguration;
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

    private CSVDirConfiguration conf = null;

    private Class connectorClass = null;

    public CSVDirSchema(
            final Class connectorClass, final CSVDirConfiguration conf) {
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

        for (String fieldName : conf.getFields()) {

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

        // set it to object class account..
        bld.defineObjectClass(ObjectClass.ACCOUNT_NAME, attrInfos);

        // return the new schema object..
        return bld.build();
    }
}
