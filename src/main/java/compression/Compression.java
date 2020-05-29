package compression;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.junit.Test;

import java.io.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Locale;

/**
 * @Description TODO
 * @Author talent2333
 * @Date 2020/5/27 14:15
 */
public class Compression {

    @Test
    public void compressTest() throws IOException {
        compress("d:/data/copy.txt",
                "org.apache.hadoop.io.compress.BZip2Codec");
    }

    @Test
    public void deCompressTest() throws IOException {
        deCompress("d:/data/copy.txt.deflate","d:/data/copypy.txt");
    }

    private void compress(String name,String method) throws IOException {
        //inputstream
        FileInputStream fis = new FileInputStream(new File(name));
        Configuration config =new Configuration();

        CompressionCodecFactory factory = new CompressionCodecFactory(config);
        CompressionCodec codec = factory.getCodecByName(method);
        if (codec == null){
            System.out.println("can not find codec for file:" + name);
            return;
        }
        //outputstream
        FileOutputStream fos = new FileOutputStream(
                new File(name + codec.getDefaultExtension()));
        //compressed outputstream
        CompressionOutputStream cos = codec.createOutputStream(fos);
        //copy stream
        IOUtils.copyBytes(fis, cos, config);
        //close stream
        IOUtils.closeStream(cos);
        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);
    }
    private void deCompress(String name,String desc) throws IOException {

        FileInputStream fis = new FileInputStream(name);

        Configuration config = new Configuration();
        CompressionCodecFactory factory = new CompressionCodecFactory(config);
        CompressionCodec codec = factory.getCodec(new Path(name));
        if (codec==null){
            System.out.println("can not find codec!");
            return;
        }
        //get compression inputstream
        CompressionInputStream cis = codec.createInputStream(fis);
        FileOutputStream fos = new FileOutputStream(new File(desc));

        IOUtils.copyBytes(cis,fos,config);
        IOUtils.closeStream(fos);
        IOUtils.closeStream(cis);
        IOUtils.closeStream(fis);

    }
    @Test
    public void test001(){

        LocalDateTime now = LocalDateTime.now();
        System.out.println("local tme:"+now);
        String format3 = now.format(DateTimeFormatter
                .ofPattern("yyyy-MM-dd,HH:mm:ss", Locale.ENGLISH));
        System.out.println(format3);

    }
}
