/**
 * Copyright (C) ${project.inceptionYear} ConnId (connid-dev@googlegroups.com)
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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import net.tirasa.connid.bundles.csvdir.CSVDirConfiguration;
import net.tirasa.connid.bundles.csvdir.CSVDirConnection;
import org.identityconnectors.common.StringUtil;
import org.identityconnectors.common.security.GuardedString;
import org.identityconnectors.common.security.GuardedString.Accessor;
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
            boolean toBeContinued;
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

    protected Map<String, String> getAttributeMap(final CSVDirConfiguration conf, final Set<Attribute> attrs,
            final Name name) {

        final Map<String, String> attributes = new HashMap<String, String>();

        Boolean status = null;
        for (Attribute attr : attrs) {
            final Object objValue =
                    attr.getValue() != null && !attr.getValue().isEmpty()
                    ? attr.getValue().get(0) : null;

            if (attr.is(Name.NAME)) {
                final String[] keys = conf.getKeyColumnNames();
                if (keys.length == 1) {
                    attributes.put(keys[0], name.getNameValue());
                }
            } else if (attr.is(OperationalAttributes.ENABLE_NAME)) {
                status = objValue == null ? null : (Boolean) objValue;
            } else {
                final Set<String> key = new HashSet<String>();
                final Set<String> value = new HashSet<String>();

                if (attr.is(OperationalAttributes.PASSWORD_NAME)) {
                    key.add(conf.getPasswordColumnName());

                    if (objValue != null) {
                        ((GuardedString) objValue).access(new Accessor() {

                            @Override
                            public void access(final char[] clearChars) {
                                value.add(new String(clearChars));
                            }
                        });
                    } else {
                        value.add("");
                    }
                } else {
                    key.add(attr.getName());
                    value.add(objValue == null ? null : objValue.toString());
                }

                attributes.put(key.iterator().next(), value.iterator().next());
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

                final String[] allValues = value == null
                        ? new String[] {}
                    : StringUtil.isBlank(conf.getMultivalueSeparator()) ? new String[] {value}
                        : value.split(Pattern.quote(conf.getMultivalueSeparator()), -1);

                if (name.equalsIgnoreCase(conf.getPasswordColumnName())) {
                    bld.addAttribute(AttributeBuilder.buildPassword(value.toCharArray()));
                } else if (name.equalsIgnoreCase(conf.getStatusColumn())) {
                    final boolean status = (StringUtil.isBlank(value)
                            ? conf.getDefaultStatusValue() : value).equals(conf.getEnabledStatusValue());

                    bld.addAttribute(AttributeBuilder.buildEnabled(status));
                } else {
                    bld.addAttribute(name, Arrays.asList(allValues));
                }
            }

            final Uid uid = new Uid(createUid(conf.getKeyColumnNames(), resultSet, conf.getKeyseparator()));

            bld.setUid(uid);
            bld.setName(uid.getUidValue());

        return bld.build();
    }
}
