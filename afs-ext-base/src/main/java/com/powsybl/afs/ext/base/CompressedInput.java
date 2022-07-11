/**
 * Copyright (c) 2022, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.ext.base;

import com.powsybl.commons.datasource.ReadOnlyDataSource;
import org.apache.commons.io.FilenameUtils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;

/**
 * @author Sylvain Leclerc <sylvain.leclerc@rte-france.com>
 */
public interface CompressedInput {

    String getName();

    InputStream newCompressedInputStream();

    InputStream newUncompressedInputStream();

    default ReadOnlyDataSource asDatasource() {
        return new ReadOnlyDataSource() {
            @Override
            public String getBaseName() {
                return FilenameUtils.getBaseName(getName());
            }

            @Override
            public boolean exists(String suffix, String ext) throws IOException {
                return suffix == null && getName().endsWith(ext);
            }

            @Override
            public boolean exists(String fileName) throws IOException {
                return Objects.equals(fileName, getName());
            }

            @Override
            public InputStream newInputStream(String suffix, String ext) throws IOException {
                if (exists(suffix, ext)) {
                    return newUncompressedInputStream();
                }
                return null;
            }

            @Override
            public InputStream newInputStream(String fileName) throws IOException {
                if (exists(fileName)) {
                    return newUncompressedInputStream();
                }
                return null;
            }

            @Override
            public Set<String> listNames(String regex) throws IOException {
                String name = getName();
                return name.matches(regex) ? Collections.singleton(name) : Collections.emptySet();
            }
        };
    }

}
