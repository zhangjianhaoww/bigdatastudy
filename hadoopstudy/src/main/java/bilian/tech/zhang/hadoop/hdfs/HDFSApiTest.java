package bilian.tech.zhang.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.FileInputStream;
import java.io.IOException;

public class HDFSApiTest {

    public static void main(String[] args) throws Exception {

        FileSystem fileSystem = getFileSystem();

        //readHDFSFile(fileSystem);
        writeToHDFS(fileSystem);


    }

    private static void writeToHDFS(FileSystem fileSystem) {
        String localFilePath = "C:\\Users\\haru\\Desktop\\桌面\\tt.txt";
        String hdfsFilePath = "hdfs://zhang001.bilian.tech:8020/user/haru/zhang/jian/hao/hao.txt";
        FileInputStream fileInputStream = null;
        FSDataOutputStream fsDataOutputStream = null;
        try {
            Path path = new Path(hdfsFilePath);

            if(fileSystem.exists(path)){
                fileSystem.delete(path, false);
            }

            fsDataOutputStream = fileSystem.create(path);

            fileInputStream = new FileInputStream(localFilePath);
            IOUtils.copyBytes(fileInputStream, fsDataOutputStream, 4096, false);
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            if (fileInputStream != null){
                try {
                    fileInputStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            if (fsDataOutputStream != null){
                try {
                    fsDataOutputStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }


    private static void readHDFSFile(FileSystem fileSystem) {
        String filePath = "hdfs://zhang001.bilian.tech:8020/user/haru/zhang/jian/hao/hao.txt";
        FSDataInputStream fsDataInputStream = null;
        Path path = new Path(filePath);
        try {
            fsDataInputStream = fileSystem.open(path);

            IOUtils.copyBytes(fsDataInputStream, System.out, 4096, false);
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            if (fsDataInputStream != null){
                try {
                    fsDataInputStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private static FileSystem getFileSystem() throws IOException {
        Configuration configuration = new Configuration();
        return FileSystem.get(configuration);
    }
}
