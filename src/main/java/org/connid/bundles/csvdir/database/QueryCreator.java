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
package org.connid.bundles.csvdir.database;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.connid.bundles.csvdir.utilities.QueryTemplate;
import org.connid.bundles.csvdir.utilities.Utilities;
import org.identityconnectors.framework.common.objects.Uid;

public final class QueryCreator {

    private QueryCreator() {
        // empty private constructor for utility class
    }

    private static String getWhereClause(final Uid uid, final String keySeparator, final String[] keys) {
        final StringBuilder where = new StringBuilder();

        final String[] uidKeys = uid.getUidValue().split(keySeparator);
        for (int i = 0; i < keys.length; i++) {
            where.append(keys[i]).append("=").append("'").append(uidKeys[i]).append("'");
            if (i < keys.length - 1) {
                where.append(" AND ");
            }
        }

        return where.toString();
    }

    private static Map<String, String> getKeyValueMap(final Map<String, String> valuesMap) {
        final Map<String, String> keyValueMap = new LinkedHashMap<String, String>();
        for (Map.Entry<String, String> entry : valuesMap.entrySet()) {
            if (entry.getValue() == null) {
                keyValueMap.put(entry.getKey(), "NULL");
            } else {
                keyValueMap.put(entry.getKey(), "'" + entry.getValue() + "'");
            }
        }

        return keyValueMap;
    }

    public static String insertQuery(final Map<String, String> valuesMap, final String tableName) {
        final Map<String, String> keyValueMap = getKeyValueMap(valuesMap);

        final QueryTemplate queryTemplate = new QueryTemplate("INSERT INTO {0}({1}) VALUES({2})");
        return queryTemplate.apply(tableName,
                Utilities.join(keyValueMap.keySet(), ','),
                Utilities.join(keyValueMap.values(), ','));
    }

    public static String updateQuery(
            final Map<String, String> valuesMap,
            final Uid uid,
            final String keySeparator,
            final String[] keys,
            final String tableName) {

        final Map<String, String> keyValueMap = getKeyValueMap(valuesMap);
        final List<String> set = new ArrayList<String>(keyValueMap.size());
        for (Map.Entry<String, String> entry : keyValueMap.entrySet()) {
            set.add(entry.getKey() + "=" + entry.getValue());
        }

        final QueryTemplate queryTemplate = new QueryTemplate("UPDATE {0} SET {1} WHERE {2}");
        return queryTemplate.apply(tableName,
                Utilities.join(set, ','),
                getWhereClause(uid, keySeparator, keys));
    }

    public static String deleteQuery(
            final Uid uid,
            final String keySeparator,
            final String[] keys,
            final String tableName) {

        final QueryTemplate queryTemplate = new QueryTemplate("DELETE FROM {0} WHERE {1}");
        return queryTemplate.apply(tableName,
                getWhereClause(uid, keySeparator, keys));
    }
}
