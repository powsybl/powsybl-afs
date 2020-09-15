/**
 * Copyright (c) 2017, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.ext.base;

import com.google.common.collect.ImmutableList;
import com.powsybl.commons.datasource.DataSource;
import com.powsybl.commons.datasource.ReadOnlyDataSource;
import com.powsybl.commons.datastore.DataFormat;
import com.powsybl.commons.datastore.DataPack;
import com.powsybl.commons.datastore.NonUniqueResultException;
import com.powsybl.commons.datastore.ReadOnlyDataStore;
import com.powsybl.iidm.import_.Importer;
import com.powsybl.iidm.network.Network;
import com.powsybl.iidm.network.NetworkFactory;
import com.powsybl.iidm.parameters.Parameter;
import com.powsybl.iidm.parameters.ParameterType;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;

/**
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at rte-france.com>
 */
public class TestImporter implements Importer {

    public static final String FORMAT = "TEST";
    public static final String EXT = "tst";

    private final Network network;

    public TestImporter(Network network) {
        this.network = Objects.requireNonNull(network);
    }

    @Override
    public String getFormat() {
        return FORMAT;
    }

    @Override
    public String getComment() {
        return "Test format";
    }

    @Override
    public List<Parameter> getParameters() {
        return ImmutableList.of(new Parameter("param1", ParameterType.BOOLEAN, "", Boolean.TRUE),
                new Parameter("param2", ParameterType.STRING, "", "value"));
    }

    @Override
    public boolean exists(ReadOnlyDataSource dataSource) {
        try {
            return dataSource.exists(null, EXT);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public Network importData(ReadOnlyDataSource dataSource, Properties parameters) {
        return network;
    }

    @Override
    public void copy(ReadOnlyDataSource fromDataSource, DataSource toDataSource) {
    }

    @Override
    public boolean exists(ReadOnlyDataStore dataStore, String fileName) {
        TestDataFormat df = new TestDataFormat(getFormat());
        try {
            Optional<DataPack> dp = df.newDataResolver().resolve(dataStore, fileName, null);
            return dp.isPresent();
        } catch (IOException | NonUniqueResultException e) {
            return false;
        }
    }

    @Override
    public Network importDataStore(ReadOnlyDataStore dataStore, String fileName, Properties parameters) {
        return network;
    }

    @Override
    public DataFormat getDataFormat() {
        return new TestDataFormat("tst");
    }

    @Override
    public Network importDataPack(DataPack dataPack, NetworkFactory networkFactory, Properties parameters) {
        return network;
    }

}
