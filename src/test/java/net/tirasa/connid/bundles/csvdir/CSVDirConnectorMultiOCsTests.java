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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import org.identityconnectors.common.security.GuardedString;
import org.identityconnectors.framework.api.ConnectorFacade;
import org.identityconnectors.framework.common.objects.Attribute;
import org.identityconnectors.framework.common.objects.AttributeBuilder;
import org.identityconnectors.framework.common.objects.AttributeUtil;
import org.identityconnectors.framework.common.objects.ConnectorObject;
import org.identityconnectors.framework.common.objects.Name;
import org.identityconnectors.framework.common.objects.ObjectClass;
import org.identityconnectors.framework.common.objects.OperationalAttributes;
import org.identityconnectors.framework.common.objects.Uid;
import org.junit.Assert;
import org.junit.Test;

public class CSVDirConnectorMultiOCsTests extends AbstractTest {

    @Test
    public final void createUpdateDeleteMultiOCs() throws IOException {
        createFile("createAccountTestMOCs", Collections.<TestAccount>emptyList());

        final CSVDirConnector connector = new CSVDirConnector();
        connector.init(createMultiOCsConfiguration("createAccountTestMOCs.*\\.csv"));
        final Name name = new Name("___mperro123;pmassi");

        final Uid employee = connector.create(
                new ObjectClass("_EMPLOYEE_"), setEmployee(setAccountId(buildTestAttributes(name))), null);
        assertEquals(name.getNameValue(), employee.getUidValue());

        final Uid manager = connector.create(
                new ObjectClass("_MANAGER_"), setManager(setAccountId(buildTestAttributes(name))), null);
        assertEquals(name.getNameValue(), manager.getUidValue());

        // --------------------------------
        // check creation result
        // --------------------------------
        final ConnectorFacade facade = createMultiOCsFacade("createAccountTestMOCs.*\\.csv");
        ConnectorObject object = facade.getObject(new ObjectClass("_EMPLOYEE_"), employee, null);
        assertNotNull(object);
        object = facade.getObject(new ObjectClass("_MANAGER_"), employee, null);
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
                new ObjectClass("_EMPLOYEE_"),
                "___mperro123;pmassi",
                new GuardedString("password".toCharArray()),
                null);

        assertNotNull(uid);

        Set<Attribute> attributes = new HashSet<Attribute>();
        attributes.add(AttributeBuilder.build(TestAccountsValue.EMAIL, "newemail@tirasa.net"));

        Uid updatedAccount = connector.update(new ObjectClass("_EMPLOYEE_"), uid, attributes, null);
        Assert.assertEquals(uid.getUidValue(), updatedAccount.getUidValue());

        object = facade.getObject(new ObjectClass("_EMPLOYEE_"), employee, null);
        assertEquals("newemail@tirasa.net",
                object.getAttributeByName(TestAccountsValue.EMAIL).getValue().get(0));

        object = facade.getObject(new ObjectClass("_MANAGER_"), employee, null);
        assertNotEquals("newemail@tirasa.net",
                object.getAttributeByName(TestAccountsValue.EMAIL).getValue().get(0));

        connector.delete(new ObjectClass("_EMPLOYEE_"), uid, null);
        object = facade.getObject(new ObjectClass("_EMPLOYEE_"), employee, null);
        assertNull(object);

        connector.delete(new ObjectClass("_MANAGER_"), uid, null);
        object = facade.getObject(new ObjectClass("_EMPLOYEE_"), employee, null);
        assertNull(object);

        connector.dispose();
    }
}
