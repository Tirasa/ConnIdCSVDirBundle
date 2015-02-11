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
package org.connid.bundles.csvdir.utilities;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import org.identityconnectors.common.StringUtil;
import org.identityconnectors.common.security.GuardedString;

public class AttributeValue {

    private final List<Object> value;

    public AttributeValue(final List<Object> value) {
        this.value = value == null || value.isEmpty() ? null : value;
    }

    public AttributeValue(final String strValue, final String multivaluDelimiter) {
        if (StringUtil.isBlank(strValue)) {
            value = null;
        } else if (StringUtil.isBlank(multivaluDelimiter)) {
            value = new ArrayList<Object>();
            value.add(strValue);
        } else {
            value = new ArrayList<Object>();
            for (String str : strValue.split(Pattern.quote(multivaluDelimiter), -1)) {
                value.add(str);
            }
        }
    }

    public Boolean toBoolean() {
        return value == null ? null : Boolean.class.cast(value.get(0));
    }

    public String toSecureString() {
        if (value == null) {
            return "";
        } else {
            final List<String> res = new ArrayList<String>();

            ((GuardedString) value.get(0)).access(new GuardedString.Accessor() {

                @Override
                public void access(final char[] clearChars) {
                    res.add(new String(clearChars));
                }
            });

            return res.get(0);
        }
    }

    @Override
    public String toString() {
        return value == null ? null : value.get(0).toString();
    }

    public String toString(final String multivaluDelimiter) {
        if (value == null) {
            return null;
        } else if (StringUtil.isBlank(multivaluDelimiter)) {
            return toString();
        } else {
            final StringBuilder bld = new StringBuilder();
            for (Object item : value) {
                if (bld.length() > 0) {
                    bld.append(multivaluDelimiter);
                }
                bld.append(item.toString().trim());
            }
            return bld.toString();
        }
    }

    public List<Object> get() {
        return value;
    }
}
