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
import org.connid.csvdir.CSVDirConfiguration;
import org.connid.csvdir.CSVDirConnection;

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
}
