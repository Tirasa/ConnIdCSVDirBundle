/**
 * ====================
 *  DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
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
package org.connid.bundles.csvdir.database;

import java.util.Map;
import org.connid.bundles.csvdir.utilities.QueryTemplate;
import org.identityconnectors.framework.common.objects.Uid;

public class QueryCreator {

    public static String updateQuery(
            final Map<String, String> valuesMap,
            final Uid uid,
            final String keySeparator,
            final String[] keys,
            final String tableName) {

        final QueryTemplate queryTemplate = new QueryTemplate("UPDATE {0} SET {1} WHERE {2}");

        final StringBuilder set = new StringBuilder();

        for (String value : valuesMap.keySet()) {
            if (set.length() > 0) {
                set.append(",");
            }
            set.append(value).append("=").
                    append(value == null ? "" : "'").append(valuesMap.get(value)).append(value == null ? "" : "'");
        }

        final String[] uidKeys = uid.getUidValue().split(keySeparator);

        final StringBuilder where = new StringBuilder();

        for (int i = 0; i < keys.length; i++) {
            where.append(keys[i]).append("=").append("'").append(uidKeys[i]).append("'");
            if (i < keys.length - 1) {
                where.append(" AND ");
            }
        }
        return queryTemplate.apply(tableName, set.toString(), where.toString());
    }

    public static String deleteQuery(
            final Uid uid,
            final String keySeparator,
            final String[] keys,
            final String tableName) {

        final QueryTemplate queryTemplate =
                new QueryTemplate("DELETE FROM {0} WHERE {1}");

        final String[] uidKeys =
                uid.getUidValue().split(keySeparator);

        final StringBuilder where = new StringBuilder();

        for (int i = 0; i < keys.length; i++) {
            where.append(keys[i]).append("=").
                    append("'").append(uidKeys[i]).append("'");

            if (i < keys.length - 1) {
                where.append(" AND ");
            }
        }
        return queryTemplate.apply(tableName, where.toString());
    }

    public static String insertQuery(
            final Map<String, String> valuesMap,
            final String[] fields,
            final String deletedField,
            final String tableName) {

        final QueryTemplate queryTemplate = new QueryTemplate("INSERT INTO {0}({1}) VALUES({2})");

        final StringBuilder values = new StringBuilder();
        final StringBuilder columnName = new StringBuilder();

        for (int i = 0; i < fields.length; i++) {

            final String value = fields[i].equals(deletedField) ? "false" : valuesMap.get(fields[i]);

            if (value != null) {
                columnName.append(fields[i]);
                values.append("'").append(valuesMap.get(fields[i])).append("'");

                if (i < fields.length - 1) {
                    columnName.append(",");
                    values.append(",");
                }
            }
        }

        return queryTemplate.apply(tableName, columnName, values);
    }
}
