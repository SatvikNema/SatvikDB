package com.satvik.satvikdb.utils;

import com.satvik.satvikdb.context.Index;
import com.satvik.satvikdb.context.LsmDbIndex;
import com.satvik.satvikdb.context.SimpleDbIndex;
import com.satvik.satvikdb.model.ByteOffset;
import com.satvik.satvikdb.model.DbFilePath;
import com.satvik.satvikdb.model.FileEntry;
import com.satvik.satvikdb.model.KeyValuePair;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.sql.SQLOutput;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class GeneralUtils {
    private GeneralUtils(){

    }


    public static long numDigits(int num) {
        int digits = 0;
        while(num > 0){
            num = num / 10;
            digits++;
        }
        return digits;
    }

    public static byte[] getByteEncoding(String key, String value) {
        int len = value.length();
        String toEncode = key + " " + len + " " + value + "\n";
        return toEncode.getBytes(StandardCharsets.UTF_8);
    }

    public static byte[] getSimpleByteEncoding(String key, String value) {
        String toEncode = key + " " + value + "\n";
        return toEncode.getBytes(StandardCharsets.UTF_8);
    }

    public static ByteOffset getByteOffset(RandomAccessFile raf, KeyValuePair keyValuePair) throws IOException {
        long currentPointer = raf.getFilePointer();
        long toGoBackFrom = currentPointer-2; // currentPointer points to next new character. currentPointer-1 is '\n' on the prev line
        raf.seek(toGoBackFrom);
        // go back until '\n' is encountered
        char c = (char) raf.readByte();
        while(c != '\n'){
            raf.seek(--toGoBackFrom);
            c = (char) raf.readByte();
        }
        long keyStartByte = toGoBackFrom + 1;

        return GeneralUtils.getByteOffset(keyStartByte, keyValuePair.getKey().length(), keyValuePair.getValue().length());

    }

    public static ByteOffset getByteOffset(long keyStartByte, int keyLength, int valueLength){
        long valueLenStartByte = keyStartByte + keyLength + 1; // +1 for additional space
        long valueStartByte = valueLenStartByte + numDigits(valueLength) + 1; // +1 to account for the extra space after value length
        System.out.println("key written at: "+keyStartByte);
        System.out.println("Length written at: "+valueLenStartByte);
        System.out.println("Value written at: "+valueStartByte);
        return new ByteOffset(valueLenStartByte, valueStartByte);
    }

    public static Optional<FileEntry> fetchValueFromFile(RandomAccessFile raf, String key) {
        String str;
        FileEntry fileEntry = null;
        while (true) {
            try {
                if ((str = raf.readLine()) == null) break;
                String[] tokens = str.split(" ");

                if(key.equals(tokens[0])) {
                    KeyValuePair keyValuePair = getKeyValuePair(tokens);
                    ByteOffset byteOffset = GeneralUtils.getByteOffset(raf, keyValuePair);
                    fileEntry = FileEntry.getDefaultBuilder().keyValuePair(keyValuePair).byteOffset(byteOffset);
                    break;
                }
            } catch (IOException e) {
                System.out.println("Error while reading from file. aborting read");
            }
        }
        if(fileEntry == null){
            return Optional.empty();
        }
        return Optional.of(fileEntry);
    }

    /**
     *
     * returns key and value from an array of string
     * 1st element is always the key
     * 2nd element onwards is the value elements, split by space
     */
    public static KeyValuePair getKeyValuePair(String[] tokens) {
        if(tokens.length == 1){
            return new KeyValuePair(tokens[0], null);
        }
        if(tokens.length > 2){
            // concatenate remaining values in tokens[1] to tokens[tokens.length-1], ie. value contains spaces
            StringBuilder sb = new StringBuilder(tokens[1]);
            for(int i=2;i<tokens.length;i++){
                sb.append(" ").append(tokens[i]);
            }
            return new KeyValuePair(tokens[0], sb.toString());
        } else {
            return new KeyValuePair(tokens[0], tokens[1]);
        }
    }

    public static int getValueLength(RandomAccessFile raf, long valueLengthStart, int lengthOfValueLength) {
        String length = readFromOffset(raf, valueLengthStart, lengthOfValueLength);
        return Integer.parseInt(length);
    }

    public static String readFromOffset(RandomAccessFile raf, long offset, int n){
        byte[] result = new byte[n];
        try {
            raf.seek(offset);
            raf.read(result, 0, n);
        } catch (IOException e) {
            System.out.println("Error while reading from file. aborting read");
        }
        return new String(result, StandardCharsets.UTF_8);
    }

    public static String getValueFromByteOffset(String filePath, ByteOffset byteOffset){
        try {
            RandomAccessFile raf = new RandomAccessFile(filePath, "r");
            return getValueFromByteOffset(raf, byteOffset);
        } catch (FileNotFoundException e) {
            System.out.println("Error opening file: "+filePath+" "+e.getMessage());
        }
        return null;
    }

    public static String getValueFromByteOffset(RandomAccessFile raf, ByteOffset byteOffset) {
        int lengthOfValueLength = (int)(byteOffset.getValueStart() - byteOffset.getValueLengthStart() - 1); // -1 because there is one space after value length ending
        int bytesToRead = GeneralUtils.getValueLength(raf, (int)byteOffset.getValueLengthStart(), lengthOfValueLength);
        return readFromOffset(raf, byteOffset.getValueStart(), bytesToRead);
    }

    public static Index loadIndex(String indexFilePath) {
        Index index = new LsmDbIndex();
        List<String> indexData = null;
        try {
            indexData = Files.readAllLines(Path.of(indexFilePath));
        } catch (IOException e) {
            System.out.println("Error reading "+indexFilePath);
        }

        try {
            for (String entry : indexData) {
                String[] tokens = entry.split(" ");
                index.put(tokens[0], new ByteOffset(Long.parseLong(tokens[1]), Long.parseLong(tokens[2])));
            }
            return index;
        }catch(Exception e){
            System.out.println("error parsing the index: "+e.getMessage());
        }
        return null;
    }

    public static void saveIndex(String indexFilePath, Index index) {
        StringBuilder sb = new StringBuilder();
        index.forEach((key, byteOffset) -> {
            sb.append(key)
                    .append(" ")
                    .append(byteOffset.getValueLengthStart())
                    .append(" ")
                    .append(byteOffset.getValueStart())
                    .append("\n");
        });
        byte[] bytes = sb.toString().getBytes(StandardCharsets.UTF_8);
        try {
            Files.write(Path.of(indexFilePath), bytes);
        } catch (IOException e) {
            System.out.println("error while writing to "+indexFilePath);
        }
    }

    public static void createFileIfNotExists(String path){
        File f = new File(path);
        if(!f.exists()){
            try {
                f.createNewFile();
            } catch (IOException e) {
                System.out.println("failed to create "+path+" exception: "+e.getMessage());
            }
        }
    }

    public static void createDirsIfNotExists(String dir){
        File f = new File(dir);
        if(!f.exists()){
            f.mkdirs();
            System.out.println("creating new directory "+dir);
        }
    }

    /**
     * creates the files specified by filenames. The directory must exists
     */
    public static void createFiles(String ...fileNames) {
        for(String path:fileNames){
            try {
                Files.createFile(Path.of(path));
                System.out.println("created: "+path);
            } catch (IOException e) {
                System.out.println("Error while creating "+ path +" "+e.getMessage());
            }
        }
    }

    public static List<DbFilePath> loadFilesSortedByAgeDesc(String rootDir, String indexFilePathStartsWith, String dbFilePathStartsWith) {
        try {
            return Files.list(Path.of(rootDir))
                    .map(Path::getFileName)
                    .map(Path::toString)
                    .filter(e -> e.startsWith(indexFilePathStartsWith))
                    .sorted((o1, o2) -> {
                        long i1 = Long.parseLong(o1.split("_")[1]);
                        long i2 = Long.parseLong(o2.split("_")[1]);
                        return Long.compare(i2, i1);
                    })
                    .map(e -> {
                        long i = Long.parseLong(e.split("_")[1]);
                        String dbFileName = dbFilePathStartsWith+i;
                        return new DbFilePath(rootDir + File.separatorChar + dbFileName, rootDir + File.separatorChar + e);
                    })
                    .collect(Collectors.toList());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    public static int binarySearch(List<String> arr, String target) {
        int len = arr.size();
        int left = 0, right = len-1, mid = -1;
        while(left <= right) {
            mid = left + (right - left) / 2;
            if(arr.get(mid).equals(target)){
                return mid;
            }
            if(arr.get(mid).compareTo(target) > 0){
                right = mid - 1;
            } else {
                left = mid + 1;
            }
        }
        return left;
    }

    public static String scanFileForValue(String key, String dbFilePath, long startByte, long endByte) {
        RandomAccessFile raf = null;

        try {
            raf = new RandomAccessFile(dbFilePath, "r");
            if(endByte < 0){
                endByte = Files.size(Path.of(dbFilePath));
            }
        } catch (IOException e) {
            System.out.println("Error opening file: "+dbFilePath+" "+e.getMessage());
        }

        try {
            raf.seek(startByte);
            raf.readLine(); // first entry will always be a single value as startByte points to a value's start. Hence skip that
            char[] entry;
            String parsedKey;

            // scan until the endByte
            while(raf.getFilePointer() < endByte){
                entry = raf.readLine().toCharArray();
                parsedKey = getParsedKey(entry);
                if(parsedKey.equals(key)){
                    return getParsedValue(entry);
                }
            }
            return null;

        } catch (IOException e) {
            System.out.println("Error reading location "+startByte+" "+e.getMessage());
        } finally {
            try {
                raf.close();
            } catch (IOException e) {
                System.out.println("Error closing file "+dbFilePath);
            }
        }
        return null;
    }

    public static String getParsedValue(char[] entry) {
        int startIndex = 0;
        int spacesEncountered = 0;
        while(startIndex < entry.length){
            if (entry[startIndex] == ' ') {
                spacesEncountered++;
                if(spacesEncountered == 2){
                    startIndex++;
                    break;
                }
            }
            startIndex++;
        }
        int len = entry.length - startIndex;
        char[] res = new char[len];
        int i = 0;
        while(startIndex < entry.length){
            res[i++] = entry[startIndex++];
        }
        return new String(res);
    }

    public static String getParsedKey(char[] entry) {
        if(entry == null){
            return null;
        }
        int i = 0;
        int count = 0;
        while(i < entry.length){
            if(entry[i] == ' '){
                break;
            }
            count++;
            i++;
        }
        i = 0;
        char[] res = new char[count];
        while(i < count){
            res[i] = entry[i++];
        }
        return new String(res);
    }

    public static void deleteFiles(List<DbFilePath> filesToMerge) {
        for(DbFilePath dbFileEntity : filesToMerge) {
            Path dbFilePath = Path.of(dbFileEntity.getDbFilePath());
            Path indexFilePath = Path.of(dbFileEntity.getIndexFilePath());
            deleteFile(dbFilePath);
            deleteFile(indexFilePath);
            System.out.println("hello world");
        }
    }

    private static void deleteFile(Path path) {
        System.out.println("deleting "+path);
        try {
            Files.delete(path);
        } catch (IOException e) {
            System.out.println("Error deleting file "+path+". "+e.getMessage());
        }
    }

    public static void writeToDisk(DbFilePath dbFilePath, long startSize, List<KeyValuePair> data) {
        AtomicLong finalStartSize = new AtomicLong(startSize);
        AtomicLong currentCount = new AtomicLong(0);
        int sparseValue = 10;
        int batchSize = 10;
        Index index = new LsmDbIndex();
        AtomicReference<StringBuilder> sb = new AtomicReference<>(new StringBuilder());
        data.forEach(keyValuePair -> {
            String key = keyValuePair.getKey();
            String value = keyValuePair.getValue();
            sb.get()
                    .append(key)
                    .append(" ")
                    .append(value.length())
                    .append(" ")
                    .append(value)
                    .append("\n");
            if(currentCount.get() % batchSize == 0){
                try {
                    byte[] bytes = sb.toString().getBytes(StandardCharsets.UTF_8);
                    Files.write(Path.of(dbFilePath.getDbFilePath()), bytes, StandardOpenOption.APPEND);
                    sb.set(new StringBuilder());
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            if(currentCount.get() % sparseValue == 0) {
                ByteOffset byteOffset = GeneralUtils.getByteOffset(finalStartSize.get(), key.length(), value.length());
                index.put(key, byteOffset);
            }
            finalStartSize.getAndAdd(key.length()+numDigits(value.length()) + value.length() + 3);
            currentCount.getAndIncrement();
        });
        if(!sb.get().isEmpty()){
            try {
                byte[] bytes = sb.toString().getBytes(StandardCharsets.UTF_8);
                Files.write(Path.of(dbFilePath.getDbFilePath()), bytes, StandardOpenOption.APPEND);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        GeneralUtils.saveIndex(dbFilePath.getIndexFilePath(), index);
    }
}
