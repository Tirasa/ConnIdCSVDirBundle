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
