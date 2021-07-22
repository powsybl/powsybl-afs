/**
 * Copyright (c) 2021, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.cassandra;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.powsybl.afs.storage.check.FileSystemCheckIssue;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @author Yichen TANG <yichen.tang at rte-france.com>
 */
class DisplayTreeIssueBuilder {

    private final TreeNode root = new TreeNode("display tree");
    private final Map<String, TreeNode> nodes = new HashMap<>();
    private final Map<String, NodeInfo> infos = new HashMap<>();

    DisplayTreeIssueBuilder(ResultSet resultSet) {
        for (Row row : resultSet) {
            RowBean rowBean = new RowBean(row.getUUID(0).toString(),
                    row.getString(1),
                    row.getString(2),
                    row.getUUID(3) == null ? null : row.getUUID(3).toString());
            cacheNodeInfo(rowBean);
            nodes.computeIfAbsent(rowBean.id, TreeNode::new);
            if (rowBean.cId != null) {
                nodes.computeIfAbsent(rowBean.cId, TreeNode::new);
                nodes.get(rowBean.id).children.add(rowBean.cId);
            }
            if (row.getUUID(4) == null) {
                root.children.add(rowBean.id);
            }
        }
    }

    private void cacheNodeInfo(RowBean bean) {
        if (!infos.containsKey(bean.id)) {
            infos.put(bean.id, new NodeInfo(bean.name, bean.pseudoClass));
        }
    }

    static class NodeInfo {

        private final String name;
        private final String pseudoClass;

        NodeInfo(String name, String pseudoClass) {
            this.name = name;
            this.pseudoClass = pseudoClass;
        }

        @Override
        public String toString() {
            return name + "(" + pseudoClass + ")";
        }
    }

    static class RowBean {

        private final String id;
        private final String name;
        private final String pseudoClass;
        private final String cId;

        RowBean(String id, String name, String pseudoClass, String cId) {
            this.id = id;
            this.name = name;
            this.pseudoClass = pseudoClass;
            this.cId = cId;
        }
    }

    static class TreeNode {
        String id;
        String name;
        Set<String> children = new HashSet<>();

        TreeNode(String id) {
            this.id = id;
        }

        TreeNode(String id, String name) {
            this.id = id;
            this.name = name;
        }

        @Override
        public String toString() {
            return id + "_" + name;
        }
    }

    FileSystemCheckIssue build() {
        StringBuilder sb = new StringBuilder();
        printNode(sb, root, 0);
        return new FileSystemCheckIssue().setDescription(sb.toString());
    }

    void printNode(StringBuilder sb, TreeNode node, int level) {
        for (int i = 0; i < level - 1; i++) {
            sb.append("    ");
        }
        if (level != 0) {
            sb.append(infos.get(node.id))
                    .append("  ")
                    .append(node.id)
                    .append("\n");
        }
        int nextLevel = level + 1;
        node.children.forEach(e -> printNode(sb, nodes.get(e), nextLevel));
    }
}
