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
package net.tirasa.connid.bundles.csvdir;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.IOException;
import org.identityconnectors.framework.api.ConnectorFacade;
import org.identityconnectors.framework.common.exceptions.ConnectorException;
import org.identityconnectors.framework.common.objects.ConnectorObject;
import org.identityconnectors.framework.common.objects.ObjectClass;
import org.identityconnectors.framework.common.objects.Uid;
import org.junit.Test;

public class CSVDirConnectorDeleteTests extends AbstractTest {

    @Test
    public void delete() throws IOException {
        createFile("sample", TestAccountsValue.TEST_ACCOUNTS);

        // **test only**
        final ConnectorFacade facade = createFacade("sample.*\\.csv");

        final Uid uid = new Uid("____jpc4323435;jPenelope");

        final ConnectorObject object = facade.getObject(ObjectClass.ACCOUNT, uid, null);
        assertNotNull(object);
        assertEquals(object.getName().getNameValue(), uid.getUidValue());

        final CSVDirConnector connector = new CSVDirConnector();
        connector.init(createConfiguration("sample.*\\.csv"));
        connector.delete(ObjectClass.ACCOUNT, uid, null);

        final ConnectorObject deleteObject = facade.getObject(ObjectClass.ACCOUNT, uid, null);
        assertNull(deleteObject);
        connector.dispose();
    }

    @Test(expected = ConnectorException.class)
    public void deleteTestOfNotExistsUser() throws IOException {
        createFile("sample", TestAccountsValue.TEST_ACCOUNTS);
        final Uid uid = new Uid("____jpc4323435,jPenelo");

        final CSVDirConnector connector = new CSVDirConnector();
        connector.init(createConfiguration("sample.*\\.csv"));
        connector.delete(ObjectClass.ACCOUNT, uid, null);
        connector.dispose();
    }
}
