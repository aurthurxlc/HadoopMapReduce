package cn.gitos.aurthur.base;

/**
 * Created by Aurthur on 2017/1/12.
 * HDFS操作类
 */

import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;


public class HDFSUtil {

    /**
     * 创建文件夹
     *
     * @param folder 文件夹名
     */
    public static void mkdirs(String folder) throws IOException {
        Path path = new Path(folder);
        FileSystem fs = FileSystem.get(HadoopUtil.getConfiguration());
        if (!fs.exists(path)) {
            fs.mkdirs(path);
            System.out.println("HDFS Create: " + folder);
        }
        fs.close();
    }

    /**
     * 删除文件夹
     *
     * @param folder 文件夹名
     */
    public static void rmr(String folder) throws IOException {
        Path path = new Path(folder);
        FileSystem fs = FileSystem.get(HadoopUtil.getConfiguration());
        fs.deleteOnExit(path);
        System.out.println("HDFS Delete: " + folder);
        fs.close();
    }

    /**
     * 重命名文件
     *
     * @param src 源文件名
     * @param dst 目标文件名
     */
    public static void rename(String src, String dst) throws IOException {
        Path name1 = new Path(src);
        Path name2 = new Path(dst);
        FileSystem fs = FileSystem.get(HadoopUtil.getConfiguration());
        fs.rename(name1, name2);
        System.out.println("HDFS Rename: from " + src + " to " + dst);
        fs.close();
    }

    /**
     * 列出该路径的文件信息
     *
     * @param folder 文件夹名
     */
    public static void ls(String folder) throws IOException {
        Path path = new Path(folder);
        FileSystem fs = FileSystem.get(HadoopUtil.getConfiguration());
        FileStatus[] list = fs.listStatus(path);
        System.out.println("HDFS ls: " + folder);
        System.out.println("==========================================================");
        for (FileStatus f : list) {
            System.out.printf("name: %s, folder: %s, size: %d\n", f.getPath(), f.isDirectory(), f.getLen());
        }
        System.out.println("==========================================================");
        fs.close();
    }

    /**
     * 创建文件
     *
     * @param file    文件名
     * @param content 文件内容
     */
    public static void createFile(String file, String content) throws IOException {
        FileSystem fs = FileSystem.get(HadoopUtil.getConfiguration());
        byte[] buff = content.getBytes();
        FSDataOutputStream os = null;
        os = fs.create(new Path(file));
        os.write(buff, 0, buff.length);
        System.out.println("HDFS Create: " + file);
        os.close();
        fs.close();
    }

    /**
     * 复制本地文件到hdfs
     *
     * @param local  本地文件路径
     * @param remote hdfs目标路径
     */
    public static void copyFile(String local, String remote) throws IOException {
        FileSystem fs = FileSystem.get(HadoopUtil.getConfiguration());
        fs.copyFromLocalFile(new Path(local), new Path(remote));
        System.out.println("HDFS copy from: " + local + " to " + remote);
        fs.close();
    }

    /**
     * 从hdfs下载文件到本地
     *
     * @param remote hdfs文件路径
     * @param local  本地目标路径
     */
    public static void download(String remote, String local) throws IOException {
        Path path = new Path(remote);
        FileSystem fs = FileSystem.get(HadoopUtil.getConfiguration());
        fs.copyToLocalFile(path, new Path(local));
        System.out.println("HDFS download: from" + remote + " to " + local);
        fs.close();
    }

    /**
     * 查看hdfs文件内容
     *
     * @param remoteFile hdfs文件路径
     */
    public static void cat(String remoteFile) throws IOException {
        Path path = new Path(remoteFile);
        FileSystem fs = FileSystem.get(HadoopUtil.getConfiguration());
        FSDataInputStream fsdis = null;
        System.out.println("HDFS cat: " + remoteFile);
        fsdis = fs.open(path);
        IOUtils.copyBytes(fsdis, System.out, 4096, false);
        IOUtils.closeStream(fsdis);
        fs.close();
    }
}
