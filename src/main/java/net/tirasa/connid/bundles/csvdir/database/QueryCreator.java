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
package net.tirasa.connid.bundles.csvdir.database;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import net.tirasa.connid.bundles.csvdir.utilities.QueryTemplate;
import net.tirasa.connid.bundles.csvdir.utilities.Utilities;
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
