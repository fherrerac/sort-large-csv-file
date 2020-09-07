import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SortLargeFile {
    static String CSV_SPLIT_BY = ",";
    static String CSV_EXTENSION = ".csv";

    List<File> files = new ArrayList<>();
    int[] columnNumbers;
    String outputDirName;
    File nonSortedFile;
    long fileSizeBytes;
    long maxSplitFileSizeBytes;
    int splitIntoNumFiles;

    public static void main(String[] args) {
        /*
            Program to sort a large (GB size) csv file by some given string columns.
            The approach used is:
            a) Split the file into smaller files. One can choose how many files to split.
            b) Sort each smaller file by the given columns.
            c) Merge all the sorted files into one. It uses the concept of merging K sorted lists,
               picking from the first elements.

            // Tested with:
            https://www.kaggle.com/mkechinov/ecommerce-behavior-data-from-multi-category-store?select=2019-Nov.csv
            file size 9,006,762,395 bytes ~ 9 GB

            // rows look like
            event_time,event_type,product_id,category_id,category_code,brand,price,user_id,user_session
            2019-11-01 00:00:00 UTC,view,1003461,2053013555631882655,electronics.smartphone,xiaomi,489.07,520088904,
            4d3b30da-a5e4-49df-b1a8-ba5943f1dd33
            2019-11-01 00:00:00 UTC,view,5000088,2053013566100866035,appliances.sewing_machine,janome,293.65,530496790,
            8e5f4f83-366c-4f70-860e-ca7417414283

            // the full file downloaded above
            String nonSortedFileName = "file.csv";

            // sorted by columns 1, 4, 5 which would be:
            event_type, category_code, brand
            int[] columns = {1, 4, 5};

            // if split in 20 files
            int splitIntoNumFiles = 20;
            a) Time to split file: 38.0 sec
            b) Time to sort files: 286.0 sec
            c) Time to merge: 109.0 sec
            Total time: 433.0 sec

            // if split in 50 files
            int splitIntoNumFiles = 50;
            a) Time to split file: 45.0 sec
            b) Time to sort files: 269.0 sec
            c) Time to merge: 168.0 sec
            Total time: 482.0 sec
         */

        Options options = new Options();
        options.addOption(Option.builder("f")
                                .longOpt("file-name")
                                .desc("File to sort")
                                .hasArg()
                                .argName("FILE-NAME")
                                .required(true)
                                .build());
        options.addOption(Option.builder("n")
                                .longOpt("number-files-to-split")
                                .desc("Number of files to split")
                                .hasArg()
                                .argName("NUMBER-FILES-TO-SPLIT")
                                .required(true)
                                .build());
        options.addOption(Option.builder("c")
                                .longOpt("columns-to-sort")
                                .desc("Columns to sort by")
                                .hasArgs()
                                .argName("COLUMNS-TO-SORT-BY")
                                .required(true)
                                .build());

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = null;
        try {
            cmd = parser.parse(options, args);
        }
        catch (ParseException e) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.setOptionComparator(null);
            formatter.printHelp("sortlargefile", options);
            return;
        }

        String nonSortedFileName = cmd.getOptionValue("f");
        int splitIntoNumFiles = Integer.valueOf(cmd.getOptionValue("n"));

        String[] columnsStr = cmd.getOptionValues("c");
        int[] columns = new int[columnsStr.length];
        for (int i = 0; i < columnsStr.length; i++) {
            columns[i] = Integer.valueOf(columnsStr[i]);
        }

        SortLargeFile sortLargeFile = new SortLargeFile(nonSortedFileName, splitIntoNumFiles, columns);
        sortLargeFile.sort();
    }

    public SortLargeFile(String nonSortedFileName, int splitIntoNumFiles, int[] columnNumbers) {
        this.splitIntoNumFiles = splitIntoNumFiles;
        this.columnNumbers = columnNumbers;
        this.nonSortedFile = new File(nonSortedFileName);
        this.fileSizeBytes = nonSortedFile.length();
        this.outputDirName = nonSortedFileName.replaceAll(CSV_EXTENSION, "/");
        this.maxSplitFileSizeBytes = fileSizeBytes / splitIntoNumFiles;

        System.out.println("File name: " + nonSortedFile.getName());
        DecimalFormat formatter = new DecimalFormat("#,###");
        System.out.println("File size: " + formatter.format(fileSizeBytes) + " bytes");
        System.out.println(String.format("File split into: %d files", splitIntoNumFiles));
    }

    public void sort() {
        long start = System.currentTimeMillis();
        splitFile();
        long end = System.currentTimeMillis();
        double timeToSplit = (end - start) / 1000;
        System.out.println("a) Time to split file: " + timeToSplit + " sec");

        start = System.currentTimeMillis();
        sortFiles();
        end = System.currentTimeMillis();
        double timeToSortFiles = (end - start) / 1000;
        System.out.println("b) Time to sort files: " + timeToSortFiles + " sec");

        start = System.currentTimeMillis();
        mergeFiles();
        end = System.currentTimeMillis();
        double timeToMerge = (end - start) / 1000;
        System.out.println("c) Time to merge: " + timeToMerge + " sec");

        double totalTime = timeToSplit + timeToSortFiles + timeToMerge;
        System.out.println("Total time: " + totalTime + " sec");
    }

    private void splitFile() {
        BufferedReader br = null;
        String line = "";
        try {
            br = new BufferedReader(new FileReader(nonSortedFile));

            // remove the first line with headers in documents file
            br.readLine();

            int numBytesWritten = 0;
            int numFiles = 1;
            boolean createNewFile = true;
            FileWriter writer = null;

            File parentDir = new File(outputDirName);
            if (parentDir.exists()) {
                File[] files = parentDir.listFiles();
                if (files != null) {
                    for (File file : files) {
                        file.delete();
                    }
                }
                parentDir.delete();
            }
            parentDir.mkdirs();

            while ((line = br.readLine()) != null) {
                if (createNewFile) {
                    String newFileName = outputDirName + numFiles + CSV_EXTENSION;
                    File file = new File(newFileName);
                    file.createNewFile();
                    files.add(file);

                    writer = new FileWriter(file);
                    numFiles++;
                    numBytesWritten = 0;
                    createNewFile = false;
                }

                writer.write(line + "\n");
                numBytesWritten += line.length();

                if (numBytesWritten > maxSplitFileSizeBytes) {
                    createNewFile = true;
                    writer.close();
                }
            }
            writer.close();
        }
        catch (FileNotFoundException e) {
            e.printStackTrace();
            throw new RuntimeException("Cannot open file");
        }
        catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("Error processing file");
        }
        finally {
            if (br != null) {
                try {
                    br.close();
                }
                catch (IOException e) {
                    e.printStackTrace();
                    throw new RuntimeException("Error processing file");
                }
            }
        }
    }

    private void sortFiles() {
        for (File file : files) {
            BufferedReader br = null;
            String line = "";
            List<String[]> rows = new ArrayList<>();

            try {
                br = new BufferedReader(new FileReader(file));
                while ((line = br.readLine()) != null) {
                    String[] columns = line.split(CSV_SPLIT_BY, -1);
                    rows.add(columns);
                }

                Collections.sort(rows, new ColumnsComparator(columnNumbers));

                FileWriter writer = new FileWriter(file, false);
                for (String[] row : rows) {
                    writer.write(String.join(CSV_SPLIT_BY, row) + "\n");
                }
                writer.close();
            }
            catch (FileNotFoundException e) {
                throw new RuntimeException("Error processing feed with original documents");
            }
            catch (IOException e) {
                throw new RuntimeException("Error processing feed with original documents");
            }
            finally {
                if (br != null) {
                    try {
                        br.close();
                    }
                    catch (IOException e) {
                        throw new RuntimeException("Error processing feed with original documents");
                    }
                }
            }
        }
    }

    private class ColumnsComparator implements Comparator<String[]> {
        int[] columnNumbers;

        public ColumnsComparator(int[] columnNumbers) {
            this.columnNumbers = columnNumbers;
        }

        @Override
        public int compare(String[] columnsOne, String[] columnsTwo) {
            if (columnsOne.length != columnsTwo.length) {
                throw new RuntimeException("Cannot compare columns of different sizes");
            }
            return compare(columnsOne, columnsTwo, 0);
        }

        private int compare(String[] columnsOne, String[] columnsTwo, int column) {
            if (column >= columnsOne.length) {
                return 0;
            }

            int columnNumber = this.columnNumbers[column];
            String columnOne = columnsOne[columnNumber];
            String columnTwo = columnsTwo[columnNumber];

            int comparison;
            if (columnOne == null && columnsTwo == null) {
                comparison = 0;
            }
            else if (columnOne == null) {
                comparison = -1;
            }
            else if (columnTwo == null) {
                comparison = 1;
            }
            else {
                comparison = columnOne.compareTo(columnTwo);
            }

            if (comparison == 0 && column < columnNumbers.length - 1) {
                return compare(columnsOne, columnsTwo, column + 1);
            }
            else {
                return comparison;
            }
        }
    }

    private void mergeFiles() {
        Map<String[], BufferedReader> firstLinesMap = new HashMap<>();
        List<BufferedReader> bufferedReaders = new ArrayList<>();

        BufferedReader br;
        FileWriter writer = null;
        try {
            for (File file : files) {
                br = new BufferedReader(new FileReader(file));
                String firstLine = br.readLine();
                String[] columns = firstLine.split(CSV_SPLIT_BY, -1);
                firstLinesMap.put(columns, br);
                bufferedReaders.add(br);
            }

            List<String[]> firstLines = new ArrayList<>(firstLinesMap.keySet());

            File file = new File(outputDirName + "sorted-" + nonSortedFile.getName());
            file.createNewFile();
            writer = new FileWriter(file);

            String newLine = "";
            while (!firstLinesMap.isEmpty()) {
                Collections.sort(firstLines, new ColumnsComparator(columnNumbers));

                String[] columns = firstLines.get(0);
                firstLines.remove(0);
                writer.write(String.join(",", columns) + "\n");

                br = firstLinesMap.remove(columns);

                newLine = br.readLine();
                if (newLine != null) {
                    columns = newLine.split(CSV_SPLIT_BY, -1);
                    firstLines.add(columns);
                    firstLinesMap.put(columns, br);
                }
            }
        }
        catch (FileNotFoundException e) {
            e.printStackTrace();
            throw new RuntimeException("Cannot open file");
        }
        catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("Error processing file");
        }
        finally {
            for (BufferedReader bufferedReader : bufferedReaders) {
                try {
                    bufferedReader.close();
                }
                catch (IOException e) {
                    e.printStackTrace();
                }
            }

            for (File file : files) {
                file.delete();
            }

            try {
                writer.close();
            }
            catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
