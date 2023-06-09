package org.txazo.kafka.client.test.compress;

import org.apache.commons.io.FileUtils;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.common.utils.ByteBufferOutputStream;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * @author xiaozhou.tu
 * @date 2023/6/8
 */
public class KafkaCompressTest {

    private static final String TOPIC = "my-topic";
    private static final byte MAGIC = RecordBatch.MAGIC_VALUE_V2;
    private static final StringSerializer STRING_SERIALIZER = new StringSerializer();

    public static void main(String[] args) throws Exception {
        int times = 10000;
        String message = FileUtils.readFileToString(new File("/Users/xiaozhou.tu/Downloads/fullVpps_ES6.json"), StandardCharsets.UTF_8.name());
        byte[] messageBytes = STRING_SERIALIZER.serialize(TOPIC, message);
        System.out.println("压缩解压缩次数: " + times);
        System.out.println("压缩前消息大小: " + messageBytes.length / 1024 + "KB");
        System.out.printf("%4s %s  %s  %s  %s  %s %n", "压缩类型", "压缩后大小(KB)", "压缩比", "压缩平均耗时(ms)", "解压缩平均耗时(ms)", "解压数据是否一致");
        testCompress(messageBytes, times);
    }

    private static void compressSize(byte[] messageBytes, CompressionType compressionType, long avgCompressTime, long avgUnCompressTime) throws Exception {
        byte[] compressBytes = compress(compressionType, messageBytes);
        byte[] uncompressBytes = uncompress(compressionType, ByteBuffer.wrap(compressBytes), messageBytes.length);
        System.out.printf("%6s %12d %5d:1 %14d %16d %14s %n", compressionType.name, compressBytes.length / 1024, messageBytes.length / compressBytes.length
                , avgCompressTime, avgUnCompressTime, Arrays.equals(messageBytes, uncompressBytes) ? "一致" : "不一致");
    }

    private static void testCompress(byte[] messageBytes, int times) throws Exception {
        // 预热
        int preTimes = 1000;
        compressForMany(messageBytes, CompressionType.SNAPPY, preTimes, true);
        compressForMany(messageBytes, CompressionType.LZ4, preTimes, true);
        compressForMany(messageBytes, CompressionType.ZSTD, preTimes, true);
        compressForMany(messageBytes, CompressionType.GZIP, preTimes, true);

        compressForMany(messageBytes, CompressionType.SNAPPY, times, false);
        compressForMany(messageBytes, CompressionType.LZ4, times, false);
        compressForMany(messageBytes, CompressionType.ZSTD, times, false);
        compressForMany(messageBytes, CompressionType.GZIP, times, false);
    }

    private static void compressForMany(byte[] messageBytes, CompressionType compressionType, int times, boolean preHot) throws Exception {
        byte[] compressBytes = compress(compressionType, messageBytes);
        long startTimeMillis = System.currentTimeMillis();
        for (int i = 0; i < times; i++) {
            compressForOnce(messageBytes, compressionType);
        }
        long compressEndTimeMillis = System.currentTimeMillis();
        for (int i = 0; i < times; i++) {
            uncompressForOnce(messageBytes, compressBytes, compressionType);
        }
        long uncompressEndTimeMillis = System.currentTimeMillis();
        if (!preHot) {
            compressSize(messageBytes, compressionType, (compressEndTimeMillis - startTimeMillis) / times,
                    (uncompressEndTimeMillis - compressEndTimeMillis) / times);
        }
    }

    private static void compressForOnce(byte[] messageBytes, CompressionType compressionType) throws Exception {
        compress(compressionType, messageBytes);
    }

    private static void uncompressForOnce(byte[] messageBytes, byte[] compressBytes, CompressionType compressionType) throws Exception {
        uncompress(compressionType, ByteBuffer.wrap(compressBytes), messageBytes.length);
    }

    /**
     * 压缩
     */
    private static byte[] compress(CompressionType compressionType, byte[] messageBytes) throws Exception {
        ByteBuffer byteBuffer = ByteBuffer.allocate(messageBytes.length + 1024);
        try (DataOutputStream dataOutputStream = new DataOutputStream(compressionType.wrapForOutput(new ByteBufferOutputStream(byteBuffer), MAGIC))) {
            dataOutputStream.write(messageBytes);
        }
        byte[] compressBytes = new byte[byteBuffer.position()];
        byteBuffer.clear();
        byteBuffer.get(compressBytes, 0, compressBytes.length);
        return compressBytes;
    }

    /**
     * 解压缩
     */
    private static byte[] uncompress(CompressionType compressionType, ByteBuffer byteBuffer, int length) throws Exception {
        try (DataInputStream reader = new DataInputStream(compressionType.wrapForInput(byteBuffer, MAGIC, BufferSupplier.NO_CACHING))) {
            byte[] uncompressBytes = new byte[length];
            reader.readFully(uncompressBytes);
            return uncompressBytes;
        }
    }

}
