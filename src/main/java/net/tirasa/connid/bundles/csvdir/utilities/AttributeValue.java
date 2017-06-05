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
package net.tirasa.connid.bundles.csvdir.utilities;

import java.util.ArrayList;
import java.util.Arrays;
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
            value = new ArrayList<Object>(Arrays.asList(strValue.split(Pattern.quote(multivaluDelimiter), -1)));
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
