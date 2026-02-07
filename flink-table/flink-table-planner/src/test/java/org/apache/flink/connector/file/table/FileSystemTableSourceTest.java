/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.file.table;

import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.planner.utils.StreamTableTestUtil;
import org.apache.flink.table.planner.utils.TableTestBase;
import org.apache.flink.types.Row;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Iterator;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link FileSystemTableSource}. */
class FileSystemTableSourceTest extends TableTestBase {

    private StreamTableTestUtil util;

    @BeforeEach
    void setup() {
        util = streamTestUtil(TableConfig.getDefault());
        TableEnvironment tEnv = util.getTableEnv();

        String srcTableDdl =
                "CREATE TABLE MyTable (\n"
                        + "  a bigint,\n"
                        + "  b int,\n"
                        + "  c varchar\n"
                        + ") with (\n"
                        + " 'connector' = 'filesystem',"
                        + " 'format' = 'testcsv',"
                        + " 'path' = '/tmp')";
        tEnv.executeSql(srcTableDdl);

        String srcTableWithMetaDdl =
                "CREATE TABLE MyTableWithMeta (\n"
                        + "  a bigint,\n"
                        + "  b int,\n"
                        + "  c varchar,\n"
                        + "  filemeta STRING METADATA FROM 'file.path'\n"
                        + ") with (\n"
                        + " 'connector' = 'filesystem',"
                        + " 'format' = 'testcsv',"
                        + " 'path' = '/tmp')";
        tEnv.executeSql(srcTableWithMetaDdl);

        String sinkTableDdl =
                "CREATE TABLE MySink (\n"
                        + "  a bigint,\n"
                        + "  b int,\n"
                        + "  c varchar\n"
                        + ") with (\n"
                        + "  'connector' = 'values',\n"
                        + "  'table-sink-class' = 'DEFAULT')";
        tEnv.executeSql(sinkTableDdl);
    }

    @Test
    void testFilterPushDown() {
        util.verifyRelPlanInsert("insert into MySink select * from MyTable where a > 10");
    }

    @Test
    void testMetadataReading() {
        util.verifyRelPlanInsert(
                "insert into MySink(a, b, c) select a, b, filemeta from MyTableWithMeta");
    }

    @ParameterizedTest(name = "extractFileName({0}) -> {1}")
    @MethodSource("fileNameCases")
    void testFileNameExtraction(String rawPath, String expected) {
        String extractedFileName = FileSystemTableSource.extractFileName(new Path(rawPath));
        assertThat(extractedFileName).isEqualTo(expected);
    }

    @ParameterizedTest(name = "file.name accessor for {0}")
    @MethodSource("fileNameCases")
    void testFileNameMetadataAccessor(String rawPath, String expected) {
        FileSourceSplit split =
                new FileSourceSplit("test-split", new Path(rawPath), 0L, 1L, 0L, 1L);

        Object actual =
                FileSystemTableSource.ReadableFileInfo.FILENAME.getAccessor().getValue(split);

        assertThat(actual).isEqualTo(StringData.fromString(expected));
    }

    static Stream<Arguments> fileNameCases() {
        return Stream.of(
                Arguments.of("file:/D:/AI-Book/FlinkApplication/data/input/user.csv", "user.csv"),
                Arguments.of("file:/D:/tmp/input/test.csv", "test.csv"),
                Arguments.of("file:/C:/Users/me/Desktop/thing.txt", "thing.txt"),
                Arguments.of("file:///tmp/input/user.csv", "user.csv"),
                Arguments.of("file:/tmp/input/dir/", "dir"),
                Arguments.of("file://localhost/tmp/input/user.csv", "user.csv"),
                Arguments.of("s3://bucket/a/b/c.parquet", "c.parquet"),
                Arguments.of("/tmp/input/dir/file.txt", "file.txt"));
    }

    /* Temporary test case to reproduce OS-specific file name resolution (FLINK-38936) */
    @TempDir java.nio.file.Path tempDir;

    @Test
    @EnabledOnOs(OS.WINDOWS)
    void testForWindowsSpecificFileNameFix() throws Exception {
        // Set up temporary directory and related input file
        java.nio.file.Path inputDir = tempDir.resolve("data").resolve("input");
        Files.createDirectories(inputDir);
        Files.write(inputDir.resolve("user.csv"), "1,Alice,30\n".getBytes(StandardCharsets.UTF_8));

        // Test environment
        TableEnvironment tEnv = util.getTableEnv();

        // Sanitize path
        String sanitizedPath = inputDir.toAbsolutePath().toString().replace("\\", "\\\\");

        tEnv.executeSql(
                "CREATE TABLE fs_source_table (\n"
                        + "  user_id STRING,\n"
                        + "  name STRING,\n"
                        + "  age INT,\n"
                        + "  file_name STRING NOT NULL METADATA FROM 'file.name' VIRTUAL\n"
                        + ") WITH (\n"
                        + "  'connector' = 'filesystem',\n"
                        + "  'path' = '"
                        + sanitizedPath
                        + "',\n"
                        + "  'format' = 'testcsv'\n"
                        + ")");

        TableResult result =
                tEnv.executeSql("SELECT user_id, name, age, file_name FROM fs_source_table");

        Iterator<Row> it = result.collect();
        Row r = it.next();

        assertThat(r.getField(3)).isEqualTo("user.csv");
    }
}
