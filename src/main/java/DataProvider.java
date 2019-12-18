import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;

public class DataProvider {

	private static final String String = null;
	//copying row DATA to Hadoop HDFS Directory in order to convert it to RDDs 
	 public void copyToHDFSDir(String srcFilePath,String destFilePath) {
		Configuration hadoopConf = new Configuration();
		FileSystem hdfs;
		Path srcPath = new Path(srcFilePath);
		Path destPath = new Path(destFilePath);
		try {
			hdfs = FileSystem.get(hadoopConf);
			hdfs.copyFromLocalFile(srcPath, destPath);

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		}
	 //extracting files from Archives 
    public String getCSV(String fileIn ,String fileOut) {
    	
        try {
            FileInputStream in = new FileInputStream(fileIn);
            FileOutputStream out = new FileOutputStream(fileOut);
            BZip2CompressorInputStream bzIn = new BZip2CompressorInputStream(in);
            int buffersize=2024;
			final byte[] buffer = new byte[buffersize];
            int n = 0;
            while (-1 != (n = bzIn.read(buffer))) {
                out.write(buffer, 0, n);
            }
            out.close();
            bzIn.close();
            System.out.println("Extraction is Done");
        }catch (FileNotFoundException e){
            System.out.println(e+"   ");

        } catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return  fileOut;
    }
}
