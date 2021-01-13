/**
 * Copyright (c) 2021, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.postgres;

import com.powsybl.afs.postgres.jpa.*;
import com.powsybl.afs.storage.NodeGenericMetadata;
import com.powsybl.afs.storage.NodeInfo;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Optional;

import static org.mockito.Mockito.mock;

/**
 * @author Yichen TANG <yichen.tang at rte-france.com>
 */
@ExtendWith(SpringExtension.class)
@SpringBootTest(classes = {PostgresAppStorageTest.class})
@EnableAutoConfiguration
class PostgresAppStorageTest {

    @Autowired
    private NodeRepository nodeRepository;
    @Autowired
    private NodeDataRepository nodeDataRepository;
    @Autowired
    private MetaStringRepository strRepo;
    @Autowired
    private MetaDoubleRepository douRepo;
    @Autowired
    private MetaIntRepository intRepo;
    @Autowired
    private TimeSeriesMetadataRepository tsRepo;
    @Autowired
    private MetaBooleanRepository booRepo;

    @Test
    void testNode() {
        MetaDataService metaDataService = new MetaDataService(strRepo, douRepo, intRepo, booRepo);
        NodeService nodeService = new NodeService(nodeRepository, metaDataService);
        PostgresAppStorage storage = new PostgresAppStorage(nodeService, nodeDataRepository, tsRepo);
        final NodeInfo rootNode = storage.createRootNodeIfNotExists("root", "folder");
        System.out.println(rootNode);
        storage.setDescription(rootNode.getId(), "new desc");
        System.out.println(storage.getNodeInfo(rootNode.getId()));

        System.out.println("testing child node");
        final List<NodeInfo> childNodes = storage.getChildNodes(rootNode.getId());
        System.out.println(childNodes.size());
        System.out.println(storage.createNode(rootNode.getId(), "child", "folder", "desc-child", 0, new NodeGenericMetadata().setInt("foo", 1).setString("str_foo", "foo")));
        System.out.println(storage.getChildNodes(rootNode.getId()).size());
        final Optional<NodeInfo> child = storage.getChildNode(rootNode.getId(), "child");
        final NodeInfo childNode = child.get();
        System.out.println("Get child:" + child.get());
        System.out.println("Get parent:" + storage.getParentNode(childNode.getId()).get());

//        System.out.println("deleting child node");
//        storage.createNode(childNode.getId(), "c3", "folder", "s", 0, new NodeGenericMetadata());
//        System.out.println(storage.getChildNodes(childNode.getId()));
//        final String parentId = storage.deleteNode(childNode.getId());
//        System.out.println(parentId);

        System.out.println("test binary data");
        try (OutputStream os = storage.writeBinaryData(childNode.getId(), "test")) {
            IOUtils.copy(new ByteArrayInputStream("Hi".getBytes()), os);
        } catch (IOException e) {
            e.printStackTrace();
        }

        storage.readBinaryData(childNode.getId(), "test")
                .ifPresent(inputStream -> {
                    final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
                    try {
                        IOUtils.copy(inputStream, byteArrayOutputStream);
                        System.out.println(byteArrayOutputStream.toString("UTF-8"));
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });
        storage.close();
    }

    @Test
    public void testTimeseries() {
        TimeSeriesService timeSeriesService = new TimeSeriesService(tsRepo);
        PostgresAppStorage storage = new PostgresAppStorage(mock(NodeService.class), mock(NodeDataRepository.class), timeSeriesService);
    }
}
