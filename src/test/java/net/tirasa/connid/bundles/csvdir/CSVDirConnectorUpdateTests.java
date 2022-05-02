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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.identityconnectors.framework.api.APIConfiguration;
import org.identityconnectors.framework.api.ConnectorFacade;
import org.identityconnectors.framework.api.ConnectorFacadeFactory;
import org.identityconnectors.framework.common.exceptions.ConnectorException;
import org.identityconnectors.framework.common.objects.Attribute;
import org.identityconnectors.framework.common.objects.AttributeBuilder;
import org.identityconnectors.framework.common.objects.ConnectorObject;
import org.identityconnectors.framework.common.objects.ObjectClass;
import org.identityconnectors.framework.common.objects.OperationOptionsBuilder;
import org.identityconnectors.framework.common.objects.Uid;
import org.identityconnectors.framework.impl.api.APIConfigurationImpl;
import org.identityconnectors.framework.impl.api.local.JavaClassProperties;
import org.identityconnectors.test.common.TestHelpers;
import org.junit.jupiter.api.Test;

public class CSVDirConnectorUpdateTests extends AbstractTest {

    private static final String NEWMAIL = "newmail@newmail.com";

    @Test
    public final void updateTest() throws IOException {
        createFile("sample", TestAccountsValue.TEST_ACCOUNTS);

        // **test only**        
        final ConnectorFacade facade = createFacade("sample.*\\.csv");

        Uid uid = new Uid("____jpc4323435;jPenelope");

        ConnectorObject object = facade.getObject(ObjectClass.ACCOUNT, uid, null);

        assertNotNull(object);
        assertEquals(object.getName().getNameValue(), uid.getUidValue());

        final CSVDirConnector connector = new CSVDirConnector();
        connector.init(createConfiguration("sample.*\\.csv"));

        Uid updatedAccount = connector.update(
                ObjectClass.ACCOUNT, uid, createSetOfAttributes(), null);

        assertEquals(uid.getUidValue(), updatedAccount.getUidValue());

        ConnectorObject objectUpdated = facade.getObject(ObjectClass.ACCOUNT, uid, null);

        assertNotNull(object);
        assertEquals(NEWMAIL,
                objectUpdated.getAttributeByName(TestAccountsValue.EMAIL).getValue().get(0));

        connector.dispose();
    }

    private Set<Attribute> createSetOfAttributes() {
        Set<Attribute> attributes = new HashSet<>();
        attributes.add(AttributeBuilder.build(TestAccountsValue.EMAIL, NEWMAIL));
        return attributes;
    }

    @Test
    public final void updateTestOfNotExistsUser() throws IOException {
        createFile("sample", TestAccountsValue.TEST_ACCOUNTS);
        Uid uid = new Uid("____jpc4323435;jPenelo");

        final CSVDirConnector connector = new CSVDirConnector();
        connector.init(createConfiguration("sample.*\\.csv"));
        assertThrows(
                ConnectorException.class,
                () -> connector.update(ObjectClass.ACCOUNT, uid, createSetOfAttributes(), null));
        connector.dispose();
    }

    @Test
    public void issueCSVDIR12() throws IOException {
        final CSVDirConfiguration config = createConfiguration("issueCSVDIR12.*\\.csv");
        config.setMultivalueSeparator("|");
        config.validate();

        createFile("issueCSVDIR12", TestAccountsValue.TEST_ACCOUNTS);

        final ConnectorFacadeFactory factory = ConnectorFacadeFactory.getInstance();
        final APIConfiguration impl = TestHelpers.createTestConfiguration(CSVDirConnector.class, config);

        // TODO: remove the line below when using ConnId >= 1.4.0.1
        ((APIConfigurationImpl) impl).
                setConfigurationProperties(JavaClassProperties.createConfigurationProperties(config));

        final ConnectorFacade connector = factory.newInstance(impl);

        Uid uid = new Uid("____jpc4323435;jPenelope");

        final ConnectorObject object = connector.getObject(ObjectClass.ACCOUNT, uid, null);

        assertNotNull(object);
        assertEquals(object.getName().getNameValue(), uid.getUidValue());

        final Set<Attribute> attributes = new HashSet<>();
        attributes.add(AttributeBuilder.build(TestAccountsValue.EMAIL, "mrossi1@tirasa.net", "mrossi2@tirasa.net"));

        final Uid updatedAccount = connector.update(ObjectClass.ACCOUNT, uid, attributes, null);
        assertEquals(uid.getUidValue(), updatedAccount.getUidValue());

        final OperationOptionsBuilder oob = new OperationOptionsBuilder();
        oob.setAttributesToGet(TestAccountsValue.EMAIL);

        final ConnectorObject obj = connector.getObject(ObjectClass.ACCOUNT, uid, oob.build());
        final List<Object> value = obj.getAttributeByName(TestAccountsValue.EMAIL).getValue();
        assertEquals(2, value.size(), 0);
        assertEquals("mrossi1@tirasa.net", value.get(0).toString());
        assertEquals("mrossi2@tirasa.net", value.get(1).toString());
    }
}
