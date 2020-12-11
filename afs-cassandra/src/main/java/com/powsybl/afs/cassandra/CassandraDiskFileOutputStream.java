/**
 * Copyright (c) 2020, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.cassandra;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * @author Yichen TANG <yichen.tang at rte-france.com>
 */
class CassandraDiskFileOutputStream extends OutputStream {

    private final FileOutputStream fos;

    CassandraDiskFileOutputStream(Path rootDir, String fileSystemName, String nodeId, String name) {
        final Path resolve = rootDir.resolve(fileSystemName).resolve(nodeId);
        try {
            Files.createDirectories(resolve);
            this.fos = new FileOutputStream(resolve.resolve(name).toFile());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void write(int b) throws IOException {
        fos.write(b);
    }
}
