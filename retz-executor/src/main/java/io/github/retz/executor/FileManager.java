/**
 *    Retz
 *    Copyright (C) 2016 Nautilus Technologies, KK.
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
package io.github.retz.executor;

import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.ArchiveException;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.ArchiveStreamFactory;
import org.apache.commons.io.FilenameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.FileSystemException;
import java.nio.file.Paths;
import java.util.List;
import java.util.zip.GZIPInputStream;

// @doc a set of functions that download, extract files from remote object storage like HDFS or HTTP web.
// This checks local directory whether it already downloaded or not.
public class FileManager {
    private static final Logger LOG = LoggerFactory.getLogger(FileManager.class);

    static void fetchPersistentFiles(List<String> files, String destination, boolean trustPVFiles) throws IOException {
        for (String file : files) {
            java.nio.file.Path path = Paths.get(file).getFileName();
            if (path == null) {
                throw new FileSystemException(destination);
            }
            File f = new File(FilenameUtils.concat(destination, path.toString()));
            LOG.info("Downloading: {} as {}", file, f);
            if (f.exists()) {
                LOG.debug("File already exists: {}", f);
                if (!trustPVFiles) {
                    try {
                        boolean needsDecompression = needsDecompression(f, destination);
                        if (needsDecompression) {
                            decompress(f, destination);
                        } else {
                            LOG.info("File {} was correctly decompressed before. Skipping decompression.", file);
                        }
                    } catch (ArchiveException e) {
                        LOG.error("ArchiveException on {}: {}", f, e.getMessage());
                        e.printStackTrace();
                    }
                }
            } else if (file.startsWith("http")) {
                fetchHTTPFile(file, destination);
                decompress(f, destination);
            } else if (file.startsWith("hdfs://")) {
                fetchHDFSFile(file, destination);
                decompress(f, destination);
            } else if (file.startsWith("maprfs://")) {
                fetchHDFSFile(file, destination);
                decompress(f, destination);
            } else {
                LOG.error("Invalid URL scheme: {}", file);
            }
        }
    }

    private static void fetchHTTPFile(String file, String dest) throws IOException {
        URL url = new URL(file);

        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("GET");

        conn.setDoOutput(true);
        java.nio.file.Path path = Paths.get(file).getFileName();
        if (path == null) {
            throw new FileNotFoundException(file);
        }
        String filename = path.toString();
        InputStream input = null;
        try (FileOutputStream output = new FileOutputStream(dest + "/" + filename)) {
            input = conn.getInputStream();
            byte[] buffer = new byte[65536];
            int bytesRead = 0;
            while ((bytesRead = input.read(buffer)) != -1) {
                output.write(buffer, 0, bytesRead);
            }
        } catch (IOException e) {
            LOG.debug(e.getMessage());
            throw e;
        } finally {
            if (input != null) input.close();
        }
        conn.disconnect();
        LOG.info("Download finished: {}", file);
    }

    private static void fetchHDFSFile(String file, String dest) throws IOException {
        LOG.debug("Downloading {} to {} as HDFS file", file, dest);
        // TODO: make 'hadoop' command arbitrarily specifiable, but given that mesos-agent (slave) can fetch hdfs:// files, it should be also available, too
        String[] hadoopCmd = {"hadoop", "fs", "-copyToLocal", file, dest};
        LOG.debug("Command: {}", String.join(" ", hadoopCmd));
        ProcessBuilder pb = new ProcessBuilder();
        pb.command(hadoopCmd).inheritIO();

        Process p = pb.start();
        while (true) {
            try {
                int result = p.waitFor();
                if (result != 0) {
                    LOG.error("Downloading {} failed: {}", file, result);
                } else {
                    LOG.info("Download finished: {}", file);
                }
                return;
            } catch (InterruptedException e) {
                LOG.error("Download process interrupted: {}", e.getMessage()); // TODO: debug?
            }
        }
    }

    private static boolean needsDecompression(File file, String dir) throws IOException, ArchiveException {
        // To use autodetect feature of ArchiveStreamFactory, input stream must be wrapped
        // with BufferedInputStream. From commons-compress mailing list.
        LOG.debug("loading file .... {} as {}", file, file.getPath());

        boolean needsDecompression = false;
        try (ArchiveInputStream ain = createAIS(file)) {
            while (ain != null) {
                ArchiveEntry entry = ain.getNextEntry();
                if (entry == null) break;

                // LOG.debug("name: {} size:{} dir: {}", entry.getName(), entry.getSize(), entry.isDirectory());
                File f = new File(FilenameUtils.concat(dir, entry.getName()));
                // LOG.debug("{} exists: {}, size:{}, dir:{}", f, f.exists(), f.length(), f.isDirectory());
                if (f.isDirectory()) {
                    continue;
                } else if (!f.exists() || entry.getSize() != f.length()) {
                    LOG.info("File {} differs: seems not decompressed before", f);
                    needsDecompression = true;
                    break;
                }
            }
        } catch (ArchiveException e) {
            needsDecompression = true;
        }
        return needsDecompression;
    }

    private static void decompress(File file, String dir) throws IOException {
        LOG.info("{} needs decompression: starting", file);
        String[] cmd = {"tar", "xf", file.getAbsolutePath(), "-C", dir};
        ProcessBuilder pb = new ProcessBuilder().command(cmd).inheritIO();
        try {
            Process p = pb.start();
            int r = p.waitFor();
            if (r == 0) {
                LOG.info("file {} successfully decompressed", file);
            } else {
                LOG.error("Failed decompression of file {}: {}", file, r);
            }
        } catch (InterruptedException e) {
            LOG.error(e.getMessage());
        }
    }

    private static ArchiveInputStream createAIS(File file) throws FileNotFoundException, IOException, ArchiveException {
        ArchiveStreamFactory factory = new ArchiveStreamFactory();
        InputStream in = new BufferedInputStream(new FileInputStream(file));

        if (file.getName().endsWith(".tar.gz") || file.getName().endsWith(".tgz")) {
            return factory.createArchiveInputStream(ArchiveStreamFactory.TAR, new GZIPInputStream(in));
        } else if (file.getName().endsWith(".tar.bz2")
                || file.getName().endsWith(".tar.xz")) { // TODO: "tar, tbz2, txz. See mesos/src/launcher/fetcher.cpp for supported formats
            LOG.error("TODO: compression on {} must be supported", file.getName());
            throw new RuntimeException();
        }
        LOG.error("Not decompressing. File with unsupported suffix: {}", file);
        return null;
    }

}
