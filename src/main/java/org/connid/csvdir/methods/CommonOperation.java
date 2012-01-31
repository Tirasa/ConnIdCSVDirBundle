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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import org.connid.csvdir.CSVDirConfiguration;
import org.connid.csvdir.CSVDirConnection;
import org.identityconnectors.common.StringUtil;
import org.identityconnectors.common.security.GuardedString;
import org.identityconnectors.common.security.GuardedString.Accessor;
import org.identityconnectors.framework.common.objects.Attribute;
import org.identityconnectors.framework.common.objects.AttributeBuilder;
import org.identityconnectors.framework.common.objects.ConnectorObjectBuilder;
import org.identityconnectors.framework.common.objects.Name;
import org.identityconnectors.framework.common.objects.OperationalAttributes;
import org.identityconnectors.framework.common.objects.Uid;

public class CommonOperation {

    protected static Boolean userExists(final String uidString,
            final CSVDirConnection connection,
            final CSVDirConfiguration configuration)
            throws SQLException {
        ResultSet rs = connection.allCsvFiles();
        Boolean founded = Boolean.FALSE;
        Boolean toBeContinued;
        String[] keys = configuration.getKeyColumnNames();
        String[] uidKeys = uidString.split(configuration.getKeyseparator());
        while (rs.next() && !founded) {
            toBeContinued = Boolean.TRUE;
            for (int i = 0; i < keys.length && toBeContinued; i++) {
                String value = rs.getString(keys[i]);
                if (!value.equalsIgnoreCase(uidKeys[i])) {
                    toBeContinued = Boolean.FALSE;
                }
            }
            founded = toBeContinued;
        }
        return founded;
    }

    protected static String createUid(
            final String[] keys, final ResultSet rs, final String keySeparator)
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
            final CSVDirConfiguration conf, final Set<Attribute> attrs) {

        final Map<String, String> attributes = new HashMap<String, String>();

        Boolean status = null;


        for (Attribute attr : attrs) {
            final Object objValue =
                    attr.getValue() != null && !attr.getValue().isEmpty()
                    ? attr.getValue().get(0) : null;

            if (attr.is(Name.NAME)) {
                // ignore attribute
            } else if (attr.is(OperationalAttributes.ENABLE_NAME)) {
                status = objValue != null ? (Boolean) objValue : null;
            } else {
                final Set<String> key = new HashSet<String>();
                final Set<String> value = new HashSet<String>();

                if (attr.is(OperationalAttributes.PASSWORD_NAME)) {
                    key.add(conf.getPasswordColumnName());

                    if (objValue != null) {
                        ((GuardedString) objValue).access(new Accessor() {

                            @Override
                            public void access(char[] clearChars) {
                                value.add(new String(clearChars));
                            }
                        });
                    } else {
                        value.add("");
                    }
                } else {
                    key.add(attr.getName());
                    value.add(objValue != null ? objValue.toString() : "");
                }

                attributes.put(
                        key.iterator().next(),
                        value.iterator().next());
            }
        }

        if (StringUtil.isNotBlank(conf.getStatusColumn())) {
            attributes.put(
                    conf.getStatusColumn(),
                    status == null ? conf.getDefaultStatusValue() : status
                    ? conf.getEnabledStatusValue()
                    : conf.getDisabledStatusValue());
        }

        return attributes;
    }

    protected ConnectorObjectBuilder buildConnectorObject(
            final CSVDirConfiguration conf, final ResultSet rs)
            throws SQLException {

        final ConnectorObjectBuilder bld = new ConnectorObjectBuilder();

        for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
            String name = rs.getMetaData().getColumnName(i);
            String value = rs.getString(name);

            final String[] allValues = value == null
                    ? new String[]{}
                    : value.split(
                    Pattern.quote(conf.getKeyseparator()), -1);

            if (name.equalsIgnoreCase(
                    conf.getPasswordColumnName())) {

                bld.addAttribute(
                        AttributeBuilder.buildPassword(
                        value.toCharArray()));

            } else if (name.equalsIgnoreCase(conf.getStatusColumn())) {

                boolean status = (StringUtil.isBlank(value)
                        ? conf.getDefaultStatusValue() : value).equals(
                        conf.getEnabledStatusValue());

                bld.addAttribute(AttributeBuilder.buildEnabled(status));

            } else {
                bld.addAttribute(name, Arrays.asList(allValues));
            }
        }

        final Uid uid = new Uid(createUid(
                conf.getKeyColumnNames(), rs, conf.getKeyseparator()));

        bld.setUid(uid);
        bld.setName(uid.getUidValue());

        return bld;
    }
}
