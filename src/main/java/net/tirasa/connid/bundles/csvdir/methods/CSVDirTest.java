/* 
 * ====================
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 * 
 * Copyright 2011 ConnId. All rights reserved.
 * 
 * The contents of this file are subject to the terms of the Common Development
 * and Distribution License("CDDL") (the "License").  You may not use this file
 * except in compliance with the License.
 * 
 * You can obtain a copy of the License at
 * http://opensource.org/licenses/cddl1.php
 * See the License for the specific language governing permissions and limitations
 * under the License.
 * 
 * When distributing the Covered Code, include this CDDL Header Notice in each file
 * and include the License file at http://opensource.org/licenses/cddl1.php.
 * If applicable, add the following below this CDDL Header, with the fields
 * enclosed by brackets [] replaced by your own identifying information:
 * "Portions Copyrighted [year] [name of copyright owner]"
 * ====================
 */
package net.tirasa.connid.bundles.csvdir.methods;

import java.sql.ResultSet;
import java.sql.SQLException;
import net.tirasa.connid.bundles.csvdir.CSVDirConfiguration;
import net.tirasa.connid.bundles.csvdir.CSVDirConnection;
import org.identityconnectors.common.logging.Log;
import org.identityconnectors.framework.common.exceptions.ConnectorException;

public class CSVDirTest {

    private static final Log LOG = Log.getLog(CSVDirTest.class);

    private final CSVDirConnection conn;

    public CSVDirTest(final CSVDirConfiguration conf) throws ClassNotFoundException, SQLException {
        this.conn = CSVDirConnection.openConnection(conf);
    }

    public void test() {
        try {
            execute();
        } catch (Exception e) {
            LOG.error(e, "error during test connection");
            throw new ConnectorException(e);
        }
    }

    private void execute() throws SQLException {
        ResultSet resultSet = conn.allCsvFiles();
        try {
            if (resultSet == null || resultSet.wasNull()) {
                LOG.error("Test failed");
                throw new ConnectorException("Test failed");
            }
        } finally {
            if (resultSet != null) {
                resultSet.close();
            }
        }
    }
}
