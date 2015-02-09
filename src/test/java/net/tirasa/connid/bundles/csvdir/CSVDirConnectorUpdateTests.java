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
package net.tirasa.connid.bundles.csvdir;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.identityconnectors.framework.api.ConnectorFacade;
import org.identityconnectors.framework.common.exceptions.ConnectorException;
import org.identityconnectors.framework.common.objects.Attribute;
import org.identityconnectors.framework.common.objects.AttributeBuilder;
import org.identityconnectors.framework.common.objects.ConnectorObject;
import org.identityconnectors.framework.common.objects.ObjectClass;
import org.identityconnectors.framework.common.objects.Uid;
import org.junit.Assert;
import org.junit.Test;

public class CSVDirConnectorUpdateTests extends AbstractTest {

    private static final String NEWMAIL = "newmail@newmail.com";

    @Test
    public final void updateTest() throws IOException {
        createFile("sample", TestAccountsValue.TEST_ACCOUNTS);

        // **test only**        
        final ConnectorFacade facade = createFacade("sample.*\\.csv");

        Uid uid = new Uid("____jpc4323435;jPenelope");

        ConnectorObject object = facade.getObject(ObjectClass.ACCOUNT, uid, null);

        Assert.assertNotNull(object);
        Assert.assertEquals(object.getName().getNameValue(), uid.getUidValue());

        final CSVDirConnector connector = new CSVDirConnector();
        connector.init(createConfiguration("sample.*\\.csv"));

        Uid updatedAccount = connector.update(
                ObjectClass.ACCOUNT, uid, createSetOfAttributes(), null);

        Assert.assertEquals(uid.getUidValue(), updatedAccount.getUidValue());

        ConnectorObject objectUpdated =
                facade.getObject(ObjectClass.ACCOUNT, uid, null);

        Assert.assertNotNull(object);
        Assert.assertEquals(objectUpdated.getAttributeByName(
                TestAccountsValue.EMAIL).getValue().get(0), NEWMAIL);

        connector.dispose();
    }

    private Set<Attribute> createSetOfAttributes() {
        Set<Attribute> attributes = new HashSet<Attribute>();
        attributes.add(AttributeBuilder.build(TestAccountsValue.EMAIL, NEWMAIL));
        return attributes;
    }

    @Test(expected = ConnectorException.class)
    public final void updateTestOfNotExistsUser()
            throws IOException {
        createFile("sample", TestAccountsValue.TEST_ACCOUNTS);
        Uid uid = new Uid("____jpc4323435;jPenelo");

        final CSVDirConnector connector = new CSVDirConnector();
        connector.init(createConfiguration("sample.*\\.csv"));
        Uid updatedAccount = connector.update(ObjectClass.ACCOUNT, uid,
                createSetOfAttributes(), null);
        Assert.assertEquals(uid.getUidValue(), updatedAccount.getUidValue());

        connector.dispose();
    }
}
