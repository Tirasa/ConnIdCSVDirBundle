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
import org.identityconnectors.framework.common.exceptions.ConnectorException;
import org.identityconnectors.framework.common.objects.Attribute;
import org.identityconnectors.framework.common.objects.AttributeBuilder;
import org.identityconnectors.framework.common.objects.Name;
import org.identityconnectors.framework.common.objects.ObjectClass;
import org.identityconnectors.framework.common.objects.Uid;
import org.junit.Assert;
import org.junit.Test;

public class CSVDirConnectorCreateTests extends CSVDirConnectorTestsSharedMethods {

    @Test
    public final void createTest() {
        final CSVDirConnector connector = new CSVDirConnector();
        connector.init(createConfiguration("thousands.*\\.csv"));
        Name name = new Name("___mperro123;pmassi;mperrone");
        Uid newAccount = connector.create(ObjectClass.ACCOUNT,
                createSetOfAttributes(name), null);
        Assert.assertEquals(name.getNameValue(), newAccount.getUidValue());
        connector.dispose();
    }

    private Set<Attribute> createSetOfAttributes(Name name) {
        Set<Attribute> attributes = new HashSet<Attribute>();

        attributes.add(name);
        attributes.add(AttributeBuilder.build(TestAccountsValue.ACCOUNTID,
                "___mperro123"));
        attributes.add(AttributeBuilder.build(TestAccountsValue.FIRSTNAME,
                "pmassi"));
        attributes.add(AttributeBuilder.build(TestAccountsValue.LASTNAME,
                "mperrone"));
        attributes.add(AttributeBuilder.build(TestAccountsValue.EMAIL,
                "massimiliano.perrone@test.it"));
        attributes.add(AttributeBuilder.build(TestAccountsValue.CHANGE_NUMBER,
                "0"));
        attributes.add(AttributeBuilder.build(TestAccountsValue.PASSWORD,
                "password"));
        attributes.add(AttributeBuilder.build(TestAccountsValue.DELETED, "no"));
        return attributes;
    }

    @Test(expected = ConnectorException.class)
    public final void createExistingUserTest()
            throws IOException {
        createFile("sample", TestAccountsValue.TEST_ACCOUNTS);
        final CSVDirConnector connector = new CSVDirConnector();
        connector.init(createConfiguration("sample.*\\.csv"));
        Name name = new Name("____jpc4323435;jPenelope");
        Uid newAccount = connector.create(ObjectClass.ACCOUNT,
                createSetOfAttributes(name), null);
        connector.dispose();
    }
}
