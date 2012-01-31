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
package org.connid.csvdir;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.identityconnectors.framework.api.APIConfiguration;
import org.identityconnectors.framework.api.ConnectorFacade;
import org.identityconnectors.framework.api.ConnectorFacadeFactory;
import org.identityconnectors.framework.common.exceptions.ConnectorException;
import org.identityconnectors.framework.common.objects.Attribute;
import org.identityconnectors.framework.common.objects.AttributeBuilder;
import org.identityconnectors.framework.common.objects.ConnectorObject;
import org.identityconnectors.framework.common.objects.ObjectClass;
import org.identityconnectors.framework.common.objects.Uid;
import org.identityconnectors.test.common.TestHelpers;
import org.junit.Assert;
import org.junit.Test;

public class CSVDirConnectorUpdateTests extends CSVDirConnectorTestsSharedMethods {

    private static final String NEWMAIL = "newmail@newmail.com";

    @Test
    public final void updateTest()
            throws IOException {
        createFile("sample", TestAccountsValue.TEST_ACCOUNTS);

        final ConnectorFacadeFactory factory =
                ConnectorFacadeFactory.getInstance();

        // **test only**
        final APIConfiguration impl = TestHelpers.createTestConfiguration(
                CSVDirConnector.class, createConfiguration("sample.*\\.csv"));

        final ConnectorFacade facade = factory.newInstance(impl);

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
