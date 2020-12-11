/**
 * Copyright (c) 2020, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.cassandra;

import java.io.*;

/**
 * @author Yichen TANG <yichen.tang at rte-france.com>
 */
public class CassandraDiskFileInputStream extends InputStream {

    private final FileInputStream fis;

    CassandraDiskFileInputStream(File inputFile) {
        try {
            fis = new FileInputStream(inputFile);
        } catch (FileNotFoundException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public int read() throws IOException {
        return fis.read();
    }
}
