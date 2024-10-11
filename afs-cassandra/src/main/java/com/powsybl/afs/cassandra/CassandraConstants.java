/**
 * Copyright (c) 2019, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.cassandra;

/**
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at rte-france.com>
 */
public final class CassandraConstants {

    public static final String AFS_KEYSPACE = "afs";

    public static final String CHILDREN_BY_NAME_AND_CLASS = "children_by_name_and_class";
    public static final String PARENT_ID = "parent_id";
    public static final String PSEUDO_CLASS = "pseudo_class";
    public static final String CREATION_DATE = "creation_date";
    public static final String MODIFICATION_DATE = "modification_date";
    public static final String DESCRIPTION = "description";
    public static final String CONSISTENT = "consistent";
    public static final String CHILD_NAME = "child_name";
    public static final String CHILD_ID = "child_id";
    public static final String CHILD_PSEUDO_CLASS = "child_pseudo_class";
    public static final String CHILD_CREATION_DATE = "child_creation_date";
    public static final String CHILD_MODIFICATION_DATE = "child_modification_date";
    public static final String CHILD_DESCRIPTION = "child_description";
    public static final String CHILD_CONSISTENT = "child_consistent";
    public static final String CHILD_VERSION = "child_version";
    public static final String DEPENDENCIES = "dependencies";
    public static final String FROM_ID = "from_id";
    public static final String BACKWARD_DEPENDENCIES = "backward_dependencies";
    public static final String TO_ID = "to_id";
    public static final String REGULAR_TIME_SERIES = "regular_time_series";
    public static final String TIME_SERIES_NAME = "time_series_name";
    public static final String DOUBLE_TIME_SERIES_DATA_COMPRESSED_CHUNKS = "double_time_series_data_compressed_chunks";
    public static final String VERSION = "version";
    public static final String CHUNK_ID = "chunk_id";
    public static final String OFFSET = "offset";
    public static final String UNCOMPRESSED_LENGTH = "uncompressed_length";
    public static final String STEP_VALUES = "step_values";
    public static final String STEP_LENGTHS = "step_lengths";
    public static final String TIME_SERIES_DATA_CHUNK_TYPES = "time_series_data_chunk_types";
    public static final String DOUBLE_TIME_SERIES_DATA_UNCOMPRESSED_CHUNKS = "double_time_series_data_uncompressed_chunks";
    public static final String VALUES = "values";
    public static final String STRING_TIME_SERIES_DATA_COMPRESSED_CHUNKS = "string_time_series_data_compressed_chunks";
    public static final String STRING_TIME_SERIES_DATA_UNCOMPRESSED_CHUNKS = "string_time_series_data_uncompressed_chunks";
    public static final String NODE_DATA = "node_data";
    public static final String NODE_DATA_NAMES = "node_data_names";
    public static final String ID = "id";
    public static final String DATA_TYPE = "data_type";
    public static final String TIME_SERIES_TAGS = "time_series_tags";
    public static final String START = "start";
    public static final String END = "end";
    public static final String SPACING = "spacing";
    public static final String ROOT_ID = "root_id";
    public static final String FS_NAME = "fs_name";
    public static final String ROOT_NODE = "root_node";
    public static final String NAME = "name";
    public static final String MT = "mt";
    public static final String MD = "md";
    public static final String MI = "mi";
    public static final String MB = "mb";
    public static final String CMT = "cmt";
    public static final String CMD = "cmd";
    public static final String CMI = "cmi";
    public static final String CMB = "cmb";
    public static final String CHUNK_NUM = "chunk_num";
    public static final String CHUNK_TYPE = "chunk_type";
    public static final String CHUNK = "chunk";
    public static final String CHUNKS_COUNT = "chunks_count";

    private CassandraConstants() {
    }
}
