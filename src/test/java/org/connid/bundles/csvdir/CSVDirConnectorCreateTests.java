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
package org.connid.bundles.csvdir;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.identityconnectors.common.security.GuardedString;
import org.identityconnectors.framework.api.APIConfiguration;
import org.identityconnectors.framework.api.ConnectorFacade;
import org.identityconnectors.framework.api.ConnectorFacadeFactory;
import org.identityconnectors.framework.common.exceptions.ConnectorException;
import org.identityconnectors.framework.common.objects.Attribute;
import org.identityconnectors.framework.common.objects.AttributeBuilder;
import org.identityconnectors.framework.common.objects.AttributeUtil;
import org.identityconnectors.framework.common.objects.ConnectorObject;
import org.identityconnectors.framework.common.objects.Name;
import org.identityconnectors.framework.common.objects.ObjectClass;
import org.identityconnectors.framework.common.objects.OperationOptionsBuilder;
import org.identityconnectors.framework.common.objects.OperationalAttributes;
import org.identityconnectors.framework.common.objects.Uid;
import org.identityconnectors.test.common.TestHelpers;
import org.junit.Test;

public class CSVDirConnectorCreateTests extends AbstractTest {

    @Test
    public final void create()
            throws IOException {

        createFile("createAccountTest", Collections.<TestAccount>emptyList());

        final CSVDirConnector connector = new CSVDirConnector();
        connector.init(createConfiguration("createAccountTest.*\\.csv"));
        final Name name = new Name("___mperro123;pmassi");

        final Uid newAccount = connector.create(ObjectClass.ACCOUNT, setAccountId(buildTestAttributes(name)), null);
        assertEquals(name.getNameValue(), newAccount.getUidValue());

        // --------------------------------
        // check creation result
        // --------------------------------
        final ConnectorFacadeFactory factory = ConnectorFacadeFactory.getInstance();

        final APIConfiguration impl = TestHelpers.createTestConfiguration(
                CSVDirConnector.class,
                createConfiguration("createAccountTest.*\\.csv"));

        final ConnectorFacade facade = factory.newInstance(impl);
        final ConnectorObject object = facade.getObject(ObjectClass.ACCOUNT, newAccount, null);

        assertNotNull(object);

        final Attribute password = AttributeUtil.find(OperationalAttributes.PASSWORD_NAME, object.getAttributes());

        assertNotNull(password.getValue().get(0));

        ((GuardedString) password.getValue().get(0)).access(new GuardedString.Accessor() {

            @Override
            public void access(final char[] clearChars) {
                assertEquals("password", new String(clearChars));
            }
        });
        // --------------------------------

        final Uid uid = connector.authenticate(
                ObjectClass.ACCOUNT,
                "___mperro123;pmassi",
                new GuardedString("password".toCharArray()),
                null);

        assertNotNull(uid);

        connector.dispose();
    }

    @Test(expected = ConnectorException.class)
    public final void createExistingUserTest() throws IOException {
        createFile("sample", TestAccountsValue.TEST_ACCOUNTS);
        final CSVDirConnector connector = new CSVDirConnector();
        connector.init(createConfiguration("sample.*\\.csv"));

        final Name name = new Name("____jpc4323435;jPenelope");
        connector.create(ObjectClass.ACCOUNT, setAccountId(buildTestAttributes(name)), null);
        connector.dispose();
    }

    @Test
    public final void issue51() throws IOException {
        createFile("createAccountTest", Collections.<TestAccount>emptyList());

        final CSVDirConnector connector = new CSVDirConnector();
        final CSVDirConfiguration configuration = createConfiguration("createAccountTest.*\\.csv");
        configuration.setKeyColumnNames(new String[] { TestAccountsValue.ACCOUNTID });
        connector.init(configuration);

        final Name name = new Name("___mperro1234");

        Set<Attribute> attributes = buildTestAttributes(name);
        attributes.add(AttributeBuilder.build(TestAccountsValue.ACCOUNTID, "___mperro1234"));
        final Uid newAccount = connector.create(ObjectClass.ACCOUNT, attributes, null);
        assertEquals(name.getNameValue(), newAccount.getUidValue());

        // --------------------------------
        // check creation result
        // --------------------------------
        final ConnectorFacadeFactory factory = ConnectorFacadeFactory.getInstance();

        final CSVDirConfiguration newConfiguration = createConfiguration("createAccountTest.*\\.csv");
        newConfiguration.setKeyColumnNames(new String[] { TestAccountsValue.ACCOUNTID });

        final APIConfiguration impl = TestHelpers.createTestConfiguration(CSVDirConnector.class, newConfiguration);

        final ConnectorFacade facade = factory.newInstance(impl);

        final ConnectorObject object = facade.getObject(ObjectClass.ACCOUNT, newAccount, null);
        assertNotNull(object);

        final Attribute password = AttributeUtil.find(OperationalAttributes.PASSWORD_NAME, object.getAttributes());
        assertNotNull(password.getValue().get(0));

        ((GuardedString) password.getValue().get(0)).access(new GuardedString.Accessor() {

            @Override
            public void access(final char[] clearChars) {
                assertEquals("password", new String(clearChars));
            }
        });
        // --------------------------------

        final Name accountid = AttributeUtil.getNameFromAttributes(object.getAttributes());
        assertEquals("___mperro1234", accountid.getNameValue());

        final Uid uid = connector.authenticate(
                ObjectClass.ACCOUNT,
                "___mperro1234",
                new GuardedString("password".toCharArray()),
                null);

        assertNotNull(uid);

        connector.dispose();
    }

    @Test
    public void issueCSVDIR6() throws IOException {
        final CSVDirConfiguration config = createConfiguration("issueCSVDIR6.*\\.csv");
        config.setFieldDelimiter(';');
        config.validate();

        final File csv = File.createTempFile("issueCSVDIR6", ".csv", testSourceDir);
        csv.deleteOnExit();

        final CSVDirConnector connector = new CSVDirConnector();
        connector.init(config);
        final Name name = new Name("___csvdir6");

        final Set<Attribute> attributes = buildTestAttributes(name);
        attributes.add(AttributeBuilder.build(TestAccountsValue.ACCOUNTID, name.getNameValue()));
        final Uid newAccount = connector.create(ObjectClass.ACCOUNT, attributes, null);
        assertEquals(name.getNameValue(), newAccount.getUidValue());
    }

    @Test
    public void issueCSVDIR12() throws IOException {
        final CSVDirConfiguration config = createConfiguration("issueCSVDIR12.*\\.csv");
        config.setMultivalueSeparator("|");
        config.validate();

        final File csv = File.createTempFile("issueCSVDIR12", ".csv", testSourceDir);
        csv.deleteOnExit();

        final Name name = new Name("___csvdir12;pmassi");
        final Set<Attribute> attributes = buildTestAttributes(name);

        attributes.add(AttributeBuilder.build(TestAccountsValue.ACCOUNTID, "___csvdir12"));

        final Attribute email = AttributeUtil.find(TestAccountsValue.EMAIL, attributes);
        attributes.remove(email);
        attributes.add(AttributeBuilder.build(TestAccountsValue.EMAIL, "mrossi1@tirasa.net", "mrossi2@tirasa.net"));

        // -----------------------------------------
        // creae new user
        // -----------------------------------------
        final ConnectorFacadeFactory factory = ConnectorFacadeFactory.getInstance();
        final APIConfiguration impl = TestHelpers.createTestConfiguration(CSVDirConnector.class, config);
        final ConnectorFacade connector = factory.newInstance(impl);

        final Uid uid = connector.create(ObjectClass.ACCOUNT, attributes, null);
        assertEquals(name.getNameValue(), uid.getUidValue());
        // -----------------------------------------

        final OperationOptionsBuilder oob = new OperationOptionsBuilder();
        oob.setAttributesToGet(TestAccountsValue.EMAIL);

        final ConnectorObject obj = connector.getObject(ObjectClass.ACCOUNT, uid, oob.build());
        final List<Object> value = obj.getAttributeByName(TestAccountsValue.EMAIL).getValue();
        assertEquals(2, value.size(), 0);
        assertEquals("mrossi1@tirasa.net", value.get(0).toString());
        assertEquals("mrossi2@tirasa.net", value.get(1).toString());
    }

    private Set<Attribute> setAccountId(final Set<Attribute> attributes) {
        attributes.add(AttributeBuilder.build(TestAccountsValue.ACCOUNTID, "___mperro123"));
        return attributes;
    }
}
