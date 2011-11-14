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
import org.identityconnectors.framework.api.APIConfiguration;
import org.identityconnectors.framework.api.ConnectorFacade;
import org.identityconnectors.framework.api.ConnectorFacadeFactory;
import org.identityconnectors.framework.common.exceptions.ConnectorException;
import org.identityconnectors.framework.common.objects.ConnectorObject;
import org.identityconnectors.framework.common.objects.ObjectClass;
import org.identityconnectors.framework.common.objects.Uid;
import org.identityconnectors.test.common.TestHelpers;
import org.junit.Assert;
import org.junit.Test;

public class CSVDirConnectorDeleteTests extends
        CSVDirConnectorTestsSharedMethods{
    
    @Test
    public final void deleteTest() throws IOException {
        createFile("sample", TestAccountsValue.TEST_ACCOUNTS);
        
        final ConnectorFacadeFactory factory =
                ConnectorFacadeFactory.getInstance();

        // **test only**
        final APIConfiguration impl = TestHelpers.createTestConfiguration(
                CSVDirConnector.class, createConfiguration("sample.*\\.csv"));

        final ConnectorFacade facade = factory.newInstance(impl);

        Uid uid = new Uid("____jpc4323435;jPenelope");

        ConnectorObject object = facade.getObject(
                ObjectClass.ACCOUNT, uid, null);

        Assert.assertNotNull(object);
        Assert.assertEquals(object.getName().getNameValue(), uid.getUidValue());
        
        final CSVDirConnector connector = new CSVDirConnector();
        connector.init(createConfiguration("sample.*\\.csv"));
        connector.delete(ObjectClass.ACCOUNT, uid, null);
        
        ConnectorObject deleteObject = facade.getObject(
                ObjectClass.ACCOUNT, uid, null);
        
        Assert.assertNull(deleteObject);
        connector.dispose();
    }
    
    @Test (expected = ConnectorException.class)
    public final void deleteTestOfNotExistsUser() throws IOException {
        createFile("sample", TestAccountsValue.TEST_ACCOUNTS);
        Uid uid = new Uid("____jpc4323435,jPenelo");

        final CSVDirConnector connector = new CSVDirConnector();
        connector.init(createConfiguration("sample.*\\.csv"));
        connector.delete(ObjectClass.ACCOUNT, uid, null);
        connector.dispose();
    }
}