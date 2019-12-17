import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.commons.*;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
public class DataProvider {

    public void getCSV(String fileIn ,String fileOut) {
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
            System.out.println("Work Done");
            String[]  test = new String[2];
        }catch (FileNotFoundException e){
            System.out.println(e+"   ");

        } catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
}
