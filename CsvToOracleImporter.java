import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;

public class CsvToOracleImporter {

    private static Logger logger = Logger.getLogger(CsvToOracleImporter.class.getName());

    public static void main(String[] args) {
        Properties properties;
        try {
            properties = loadProperties("config.properties");
            String logFilePath = properties.getProperty("logFilePath");

            FileHandler fileHandler = new FileHandler(logFilePath);
            fileHandler.setFormatter(new SimpleFormatter());
            logger.addHandler(fileHandler);
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }

        properties = null;

        try {
            properties = loadProperties("config.properties");
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }

        String jdbcUrl = properties.getProperty("jdbcUrl");
        String username = properties.getProperty("username");
        String password = properties.getProperty("password");
        String folderPath = properties.getProperty("csvFolder"); // Add a property for the folder path
        String backupFolder = properties.getProperty("backupFolder");
        String tableName = properties.getProperty("tableName");

        File folder = new File(folderPath);
        File[] files = folder.listFiles((dir, name) -> name.toLowerCase().endsWith(".csv"));

        if (files == null) {
            System.out.println("No CSV files found in the specified folder.");
            return;
        }

        for (File file : files) {
            importCsvFile(file, jdbcUrl, username, password, backupFolder, tableName);
        }

        System.out.println("Data import completed for all CSV files in the folder!");
    }

    private static void importCsvFile(File csvFile, String jdbcUrl, String username, String password,
            String backupFolder, String tableName) {
        try {
            Connection connection = DriverManager.getConnection(jdbcUrl, username, password);
            CSVReader csvReader = new CSVReader(new FileReader(csvFile));

            List<String> header;
            try {
                String[] headerArray = csvReader.readNext();
                header = (headerArray != null) ? Arrays.asList(headerArray) : null;
            } catch (IOException | CsvValidationException e) {
                handleCsvException(csvReader, e);
                return;
            }

            String insertSql = createInsertStatement(tableName, header);
            PreparedStatement preparedStatement = connection.prepareStatement(insertSql);

            try {
                csvReader.readNext(); // Skip the header line

                String[] nextLine;
                while ((nextLine = csvReader.readNext()) != null) {
                    try {
                        for (int i = 0; i < header.size(); i++) {
                            String value = nextLine[i];
                            if (value.isEmpty()) {
                                preparedStatement.setNull(i + 1, java.sql.Types.NULL);
                            } else {
                                preparedStatement.setString(i + 1, value);
                            }
                        }

                        preparedStatement.executeUpdate();

                        logger.log(Level.INFO, "Data inserted successfully: " + String.join(", ", nextLine));
                    } catch (SQLException e) {
                        logger.log(Level.SEVERE, "Error inserting data: " + String.join(", ", nextLine), e);
                    }
                }
            } catch (IOException | CsvValidationException e) {
                handleCsvException(csvReader, e);
            } finally {
                csvReader.close();
            }

            moveCsvFile(csvFile.getAbsolutePath(), backupFolder);

            preparedStatement.close();
            connection.close();

            System.out.println("Data import completed for CSV file: " + csvFile.getName());
        } catch (SQLException | IOException e) {
            e.printStackTrace();
        }
    }

    private static void handleCsvException(CSVReader csvReader, Exception e) {
        try {
            csvReader.close();
        } catch (IOException ioException) {
            ioException.printStackTrace();
        }

        if (e instanceof CsvValidationException) {
            e.printStackTrace();
            logger.log(Level.SEVERE, "CSV Validation Exception: " + e.getMessage(), e);
        } else {
            e.printStackTrace();
        }
    }

    private static Properties loadProperties(String fileName) throws IOException {
        Properties properties = new Properties();
        FileInputStream input = new FileInputStream(fileName);

        try {
            properties.load(input);
        } catch (Throwable var6) {
            try {
                input.close();
            } catch (Throwable var5) {
                var6.addSuppressed(var5);
            }

            throw var6;
        }

        input.close();
        return properties;
    }

    private static void moveCsvFile(String sourcePath, String backupFolder) throws IOException {
        Path source = Paths.get(sourcePath);
        Path destination = Paths.get(backupFolder, source.getFileName().toString());

        Files.move(source, destination);

        System.out.println("CSV file moved to: " + destination.toString());
    }

    private static String createInsertStatement(String tableName, List<String> columns) {
        StringBuilder sql = new StringBuilder("INSERT INTO ");
        sql.append(tableName).append(" (");

        // Append column names
        for (String column : columns) {
            sql.append(column).append(", ");
        }
        sql.setLength(sql.length() - 2);  // Remove the last comma and space
        sql.append(") VALUES (");

        // Append placeholders for values
        for (int i = 0; i < columns.size(); i++) {
            sql.append("?, ");
        }
        sql.setLength(sql.length() - 2);  // Remove the last comma and space
        sql.append(")");

        return sql.toString();
    }
}
