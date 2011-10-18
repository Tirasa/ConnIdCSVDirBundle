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

import java.util.HashMap;
import java.util.Map;
import org.identityconnectors.common.EqualsHashCodeBuilder;
import org.identityconnectors.framework.common.objects.Attribute;
import org.identityconnectors.framework.common.objects.AttributeUtil;
import org.identityconnectors.framework.common.objects.ConnectorObject;

public class TestAccount {

    private String _changeNumber;

    private String _accountId;

    private String _firstName;

    private String _lastName;

    private String _email;

    private String _password;

    private String _deleted;

    public TestAccount(final String accountId, final String firstName,
            final String lastName, final String email,
            final String changeNumber, final String password,
            final String deleted) {
        _accountId = accountId;
        _firstName = firstName;
        _lastName = lastName;
        _email = email;
        _changeNumber = changeNumber;
        _password = password;
        _deleted = deleted;
    }

    public TestAccount(final ConnectorObject obj) {
        // go through each of the other variables..
        for (Attribute attr : obj.getAttributes()) {
            if (TestAccountsValue.CHANGE_NUMBER.equalsIgnoreCase(attr.getName())) {
                _changeNumber = AttributeUtil.getStringValue(attr);
            } else if (TestAccountsValue.FIRSTNAME.equalsIgnoreCase(attr.getName())) {
                _firstName = AttributeUtil.getStringValue(attr);
            } else if (TestAccountsValue.LASTNAME.equalsIgnoreCase(attr.getName())) {
                _lastName = AttributeUtil.getStringValue(attr);
            } else if (TestAccountsValue.EMAIL.equalsIgnoreCase(attr.getName())) {
                _email = AttributeUtil.getStringValue(attr);
            } else if (TestAccountsValue.ACCOUNTID.equalsIgnoreCase(attr.getName())) {
                _accountId = AttributeUtil.getStringValue(attr);
            }
        }
    }

    public String getAccountId() {
        return _accountId;
    }

    public String getFirstName() {
        return _firstName;
    }

    public String getLastName() {
        return _lastName;
    }

    public String getEmail() {
        return _email;
    }

    public String getChangeNumber() {
        return _changeNumber;
    }

    public String getDeleted() {
        return _deleted;
    }

    public String getPassword() {
        return _password;
    }

    public EqualsHashCodeBuilder getEqHash() {
        final EqualsHashCodeBuilder ret = new EqualsHashCodeBuilder();
        ret.append(getAccountId());
        ret.append(getFirstName());
        ret.append(getLastName());
        ret.append(getEmail());
        ret.append(getChangeNumber());
        return ret;
    }

    @Override
    public boolean equals(Object obj) {
        boolean ret = false;
        if (obj instanceof TestAccount) {
            ret = getEqHash().equals(((TestAccount) obj).getEqHash());
        }
        return ret;
    }

    @Override
    public int hashCode() {
        return getEqHash().hashCode();
    }

    @Override
    public String toString() {
        // poor man's to string..
        Map<String, String> map = new HashMap<String, String>();
        map.put("id", _accountId);
        map.put("changeNumber", _changeNumber);
        map.put("email", _email);
        map.put("firstName", _firstName);
        map.put("lastName", _lastName);
        return map.toString();
    }

    /**
     * Create a string representation of a field, using the textQualifier if
     * the fieldDelimiter is contained in the field's value.
     * 
     * @param field
     *            String value of field to convert/externalize.
     * @param fieldDelimiter
     *            delimiter used between fields
     * @param textQualifier
     *            text qualifier to use as needed
     * @return String representation of the field suitable for writing.
     */
    static String getField(final String field, final char fieldDelimiter,
            final char textQualifier) {
        String result = field;
        if ((field != null) && (field.indexOf(fieldDelimiter) > -1)) {
            result = textQualifier + field + textQualifier;
        }
        return result;
    }

    public String toLine(final char fieldd, final char textq) {
        StringBuilder buf = new StringBuilder();

        buf.append(getField(getAccountId(), fieldd, textq));
        buf.append(fieldd);
        buf.append(getField(getFirstName(), fieldd, textq));
        buf.append(fieldd);
        buf.append(getField(getLastName(), fieldd, textq));
        buf.append(fieldd);
        buf.append(getField(getEmail(), fieldd, textq));
        buf.append(fieldd);
        buf.append(getField(getChangeNumber(), fieldd, textq));
        buf.append(fieldd);
        buf.append(getField(getPassword(), fieldd, textq));
        buf.append(fieldd);
        buf.append(getField(getDeleted(), fieldd, textq));

        return buf.toString();
    }
}
