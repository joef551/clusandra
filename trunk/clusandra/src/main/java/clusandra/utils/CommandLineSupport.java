/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/*
 * COPYRIGHT(c) 2011 by Jose R. Fernandez
 *
 * This file is part of CluSandra.
 *
 * CluSandra is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * CluSandra is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with CluSandra.  If not, see <http://www.gnu.org/licenses/>.
 * $Date: 2011-06-27 16:14:32 -0400 (Mon, 27 Jun 2011) $
 * $Revision: 48 $
 * $Author: jose $
 * $Id: CommandLineSupport.java 48 2011-06-27 20:14:32Z jose $
 */
package clusandra.utils;

import java.util.ArrayList;

import clusandra.utils.IntrospectionSupport;

/**
 * Helper utility that can be used to set the properties on any object using
 * command line arguments.
 *
 */
public final class CommandLineSupport {

    private CommandLineSupport() {
    }

    /**
     * Sets the properties of an object given the command line args.
     *
     * @param target the object that will have it's properties set
     * @param args the commline options
     * @return any arguments that are not valid options for the target
     */
    public static String[] setOptions(Object target, String[] args) {
        ArrayList<String> rc = new ArrayList<String>();

        for (int i = 0; i < args.length; i++) {
            if (args[i] == null) {
                continue;
            }

            if (args[i].startsWith("--")) {

                // --options without a specified value are considered boolean
                // flags that are enabled.
                String value = "true";
                String name = args[i].substring(2);

                // if --option=value case
                int p = name.indexOf("=");
                if (p > 0) {
                    value = name.substring(p + 1);
                    name = name.substring(0, p);
                }

                // name not set, then it's an unrecognized option
                if (name.length() == 0) {
                    rc.add(args[i]);
                    continue;
                }

                String propName = convertOptionToPropertyName(name);
                if (!IntrospectionSupport.setProperty(target, propName, value)) {
                    rc.add(args[i]);
                    continue;
                }
            }
        }

        String r[] = new String[rc.size()];
        rc.toArray(r);
        return r;
    }

    /**
     * converts strings like: test-enabled to testEnabled
     *
     * @param name
     * @return
     */
    private static String convertOptionToPropertyName(String name) {
        String rc = "";

        // Look for '-' and strip and then convert the subsequent char to
        // uppercase
        int p = name.indexOf("-");
        while (p > 0) {
            // strip
            rc += name.substring(0, p);
            name = name.substring(p + 1);

            // can I convert the next char to upper?
            if (name.length() > 0) {
                rc += name.substring(0, 1).toUpperCase();
                name = name.substring(1);
            }

            p = name.indexOf("-");
        }
        return rc + name;
    }
}
