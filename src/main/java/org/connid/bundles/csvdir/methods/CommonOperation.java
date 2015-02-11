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
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.connid.bundles.csvdir.CSVDirConfiguration;
import org.connid.bundles.csvdir.CSVDirConnection;
import org.connid.bundles.csvdir.utilities.AttributeValue;
import org.identityconnectors.common.StringUtil;
import org.identityconnectors.framework.common.objects.Attribute;
import org.identityconnectors.framework.common.objects.AttributeBuilder;
import org.identityconnectors.framework.common.objects.ConnectorObject;
import org.identityconnectors.framework.common.objects.ConnectorObjectBuilder;
import org.identityconnectors.framework.common.objects.Name;
import org.identityconnectors.framework.common.objects.OperationalAttributes;
import org.identityconnectors.framework.common.objects.Uid;

public class CommonOperation {

    protected static Boolean userExists(final String uidString,
            final CSVDirConnection conn, final CSVDirConfiguration conf)
            throws SQLException {

        final ResultSet resultSet = conn.allCsvFiles();

        final String[] keys = conf.getKeyColumnNames();
        final String[] uidKeys = uidString.split(conf.getKeyseparator());
        try {
            boolean found = false;
            boolean toBeContinued = true;
            while (resultSet.next() && !found) {
                toBeContinued = true;
                for (int i = 0; i < keys.length && toBeContinued; i++) {
                    final String value = resultSet.getString(keys[i]);
                    if (!value.equalsIgnoreCase(uidKeys[i])) {
                        toBeContinued = false;
                    }
                }
                found = toBeContinued;
            }

            return found;
        } finally {
            resultSet.close();
        }
    }

    protected static String createUid(final String[] keys, final ResultSet rs, final String keySeparator)
            throws SQLException {

        final StringBuilder uid = new StringBuilder();

        if (keys != null && keys.length > 0) {
            for (String field : keys) {
                if (uid.length() > 0) {
                    uid.append(keySeparator);
                }
                uid.append(rs.getString(field));
            }
        }

        return uid.toString();
    }

    protected Map<String, String> getAttributeMap(
            final CSVDirConfiguration conf, final Set<Attribute> attrs, final Name name) {

        final Map<String, String> attributes = new HashMap<String, String>();

        Boolean status = null;
        for (Attribute attr : attrs) {
            final AttributeValue attrValue = new AttributeValue(attr.getValue());

            if (attr.is(Name.NAME)) {
                final String[] keys = conf.getKeyColumnNames();
                if (keys.length == 1) {
                    attributes.put(keys[0], name.getNameValue());
                }
            } else if (attr.is(OperationalAttributes.ENABLE_NAME)) {
                status = attrValue.toBoolean();
            } else {
                if (attr.is(OperationalAttributes.PASSWORD_NAME)) {
                    attributes.put(conf.getPasswordColumnName(), attrValue.toSecureString());
                } else {
                    attributes.put(attr.getName(), attrValue.toString(conf.getMultivalueSeparator()));
                }
            }
        }

        if (StringUtil.isNotBlank(conf.getStatusColumn())) {
            attributes.put(conf.getStatusColumn(),
                    status == null ? conf.getDefaultStatusValue() : status
                                    ? conf.getEnabledStatusValue()
                                    : conf.getDisabledStatusValue());
        }

        return attributes;
    }

    protected ConnectorObject buildConnectorObject(final CSVDirConfiguration conf, final ResultSet resultSet)
            throws SQLException {

        final ConnectorObjectBuilder bld = new ConnectorObjectBuilder();

        for (int i = 1; i <= resultSet.getMetaData().getColumnCount(); i++) {
            final String name = resultSet.getMetaData().getColumnName(i);
            final String value = resultSet.getString(name);

            if (name.equalsIgnoreCase(conf.getPasswordColumnName()) && StringUtil.isNotBlank(value)) {
                bld.addAttribute(AttributeBuilder.buildPassword(value.toCharArray()));
            } else if (name.equalsIgnoreCase(conf.getStatusColumn())) {
                final boolean status = (StringUtil.isBlank(value)
                        ? conf.getDefaultStatusValue() : value).equals(conf.getEnabledStatusValue());

                bld.addAttribute(AttributeBuilder.buildEnabled(status));
            } else {
                bld.addAttribute(name, new AttributeValue(value, conf.getMultivalueSeparator()).get());
            }
        }

        final Uid uid = new Uid(createUid(conf.getKeyColumnNames(), resultSet, conf.getKeyseparator()));

        bld.setUid(uid);
        bld.setName(uid.getUidValue());

        return bld.build();
    }
}
