/**
 *    Retz
 *    Copyright (C) 2016 Nautilus Technologies, Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package io.github.retz.localexecutor;


import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import spark.Spark;

import java.io.IOException;

public class FileManagerTest {
    @ClassRule
    public final static TemporaryFolder folder = new TemporaryFolder();

    @BeforeClass
    public static void initHTTPServer() {

        Spark.port(24377);
        Spark.staticFileLocation("/public");
        //Spark.secure("src/test/resources/retz-dev.jks", "retz-dev",null, null);
        Spark.init();
        Spark.awaitInitialization();
    }

    @AfterClass
    public static void stopHTTPServer() {
        Spark.stop();
    }

    @Test
    public void testHTTPDownload() throws IOException {
        /*
        File dest = folder.newFolder();
        String[] files = {"https://localhost:24377/test.html",
                "https://localhost:24377/test.txt"};
        List<String> persistentFiles = Arrays.asList(files);
        FileManager.fetchPersistentFiles(persistentFiles, dest.getAbsolutePath(), true);
        File[] localFiles = dest.listFiles();
        Assert.assertNotNull(localFiles);
        List<File> list = Arrays.asList(localFiles);

        for (File f : list) {
            System.err.println(f);
        }
        System.err.println(dest.getPath());

        Assert.assertTrue(list.contains(new File(dest.getPath() + "/test.html")));
        Assert.assertTrue(list.contains(new File(dest.getPath() + "/test.txt")));
        */
    }

    // @doc testing miniHdfsCluster works or not
    @Test
    public void testMiniDFSDownload() throws IOException { // TODO: move this test to inttest
        /*
        File localDir = folder.newFolder("local");

        String hdfsURI = "hdfs://localhost:" + hdfsCluster.getNameNodePort() + "/";

        File srcFile = new File(FilenameUtils.concat(localDir.toString(), "test1.txt"));
        FileUtils.write(srcFile, "pocketburgers", Charset.defaultCharset());

        FileSystem fs = FileSystem.get(conf);
        fs.copyFromLocalFile(new Path(srcFile.toURI()), new Path(hdfsURI));

        File checkFile = new File(FilenameUtils.concat(localDir.toString(), "test2.txt"));
        fs.copyToLocalFile(new Path(hdfsURI + "/test1.txt"), new Path(checkFile.toURI()));

        String content = FileUtils.readFileToString(checkFile, Charset.defaultCharset());
        Assert.assertEquals("pocketburgers", content);
        System.err.println(content);

        {
            fs.copyFromLocalFile(new Path(srcFile.toURI()), new Path(hdfsURI + "/test4.txt"));

            File dest = folder.newFolder();
            String[] files = {"http://localhost:24377/test.txt", hdfsURI + "test4.txt"};
            List<String> persistentFiles = Arrays.asList(files);
            FileManager.fetchPersistentFiles(persistentFiles, dest.getAbsolutePath());

            File[] localFiles = dest.listFiles();
            Assert.assertNotNull(localFiles);
            List<File> list = Arrays.asList(localFiles);
            for (File f : list) {
                System.err.println(f);
            }
            System.err.println(dest.getPath());

            Assert.assertTrue(list.contains(new File(dest.getPath() + "/test.txt")));
            Assert.assertTrue(list.contains(new File(dest.getPath() + "/test4.txt")));
        }
        */
    }
}
