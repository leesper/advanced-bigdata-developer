package com.kkb.hdfs;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Test;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;

public class HDFSOperate {
    @Test
    public void mkdirToHdfs() throws IOException {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://node1:8020");

        FileSystem fileSystem = FileSystem.get(configuration);
        boolean isOK = fileSystem.mkdirs(new Path("/mkdir0119"));

        fileSystem.close();
    }

    @Test
    public void mkdirWithUser() throws IOException, URISyntaxException, InterruptedException {
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://node1:8020"), configuration, "test");

        boolean isOK = fileSystem.mkdirs(new Path("/mkdir011901"));
        fileSystem.close();
    }

    @Test
    public void uploadFile() throws IOException {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://node1:8020");
        FileSystem fileSystem = FileSystem.get(configuration);
        fileSystem.copyFromLocalFile(
                new Path("file:///Users/likejun/advanced-bigdata-developer/3_node_hadoop_cluster/Vagrantfile"),
                new Path("hdfs://node1:8020/kaikeba/dir1"));
        fileSystem.close();
    }

    @Test
    public void downloadFile() throws IOException {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://node1:8020");
        FileSystem fileSystem = FileSystem.get(configuration);
        fileSystem.copyToLocalFile(
                new Path("hdfs://node1:8020/edits.txt"),
                new Path("file:///Users/likejun/edits.txt"));
        fileSystem.close();
    }

    @Test
    public void deleteFile() throws IOException {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://node1:8020");
        FileSystem fileSystem = FileSystem.get(configuration);
        fileSystem.delete(new Path("hdfs://node1:8020/index.html"), false);
        fileSystem.close();
    }

    @Test
    public void renameFile() throws IOException {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://node1:8020");
        FileSystem fileSystem = FileSystem.get(configuration);
        fileSystem.rename(
                new Path("hdfs://node1:8020/edits.txt"),
                new Path("hdfs://node1:8020/edit.txt"));
        fileSystem.close();
    }

    @Test
    public void showFileInfo() throws IOException, URISyntaxException {
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://node1:8020"), configuration);
        RemoteIterator<LocatedFileStatus> fileInfos = fileSystem.listFiles(new Path("/"), true);

        while (fileInfos.hasNext()) {
            LocatedFileStatus status = fileInfos.next();
            // file name
            System.out.println(status.getPath().getName());
            // file length
            System.out.println(status.getLen());
            // file permission
            System.out.println(status.getPermission());
            // file group
            System.out.println(status.getGroup());

            BlockLocation[] locations = status.getBlockLocations();

            for (BlockLocation location : locations) {
                String[] hosts = location.getHosts();
                for (String host : hosts) {
                    System.out.println(host);
                }
            }
        }

        fileSystem.close();
    }

    @Test
    public void getFileFromHDFS() throws IOException, URISyntaxException {
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://node1:8020"), configuration);

        FSDataInputStream fis = fileSystem.open(new Path("hdfs://node1:8020/edit.txt"));
        File outputFile = new File("/Users/likejun/new_edit.txt");
        outputFile.createNewFile();
        FileOutputStream fos = new FileOutputStream(outputFile, false);

        IOUtils.copy(fis, fos);
        IOUtils.closeQuietly(fis);
        IOUtils.closeQuietly(fos);
        fileSystem.close();
    }
}
