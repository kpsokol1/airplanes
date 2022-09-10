package com.planes.kyle;

import java.sql.*;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.*;

import static com.planes.kyle.Parser.*;
import static com.planes.kyle.Utilities.*;

class DB {
    static void DBConnect() {
        try {
                Class.forName(getAuth("driver")).getConstructor().newInstance();
                conn = DriverManager.getConnection(getAuth("url") + "?characterEncoding=latin1", getAuth("username"), getAuth("password"));
                conn.setCatalog(getAuth("DBName"));
        }
        catch (Exception e) {
            System.out.println("Unable to find and load driver: " + e.getMessage());
            System.out.println(LocalDateTime.now());
            System.out.println("DB1");
            Email.sendErrorEmail(e);
        }
    }

    static boolean checkIfTableExists(String tableName) {
        Statement stmt = null;
        try {
            stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SHOW TABLES LIKE '" + tableName + "'");

            //If table already exists
            if (rs.next()) return true;
        }
        catch(SQLException e) {
            System.out.println(e.getMessage());
            System.out.println(LocalDateTime.now());
            System.out.println("DB2");
            Email.sendErrorEmail(e);
        }
        finally {
            try {
                if (stmt != null) {
                    stmt.close();
                }
            }
            catch (Exception e) {
                System.out.println(e.getMessage());
                System.out.println(LocalDateTime.now());
                System.out.println("DB3");
                Email.sendErrorEmail(e);
            }
        }
        return false;
    }

    static void createTable(String tableName) throws SQLException {
        Statement stmtEL;
        StringBuilder sql = new StringBuilder();

        //Get connection info
        stmtEL = conn.createStatement();

        //Create DB table with specified table/column names
        sql.append("CREATE TABLE `" + tableName + "` (id INTEGER not NULL AUTO_INCREMENT,");

        for (Map.Entry<String, String> col : columnNames.entrySet())
        {
            sql.append(" `" + col.getKey() + "` VARCHAR (110), ");
        }
        sql.append("PRIMARY KEY (id))");

        //Write changes to DB
        stmtEL.executeUpdate(sql.toString());

        //Add index to ICAO column
        if(tableName.contains("Raw") || tableName.contains("Clean")){
            stmtEL.executeUpdate("CREATE INDEX " + tableName +"_ICAO_index on " + tableName +" (ICAO)");
        }
        stmtEL.close();
    }
    private static void populateSQLQuery(HashMap<String, String> aircraftData, String table) throws SQLException {
        String sql;
        StringJoiner sqlRowsWithValues = new StringJoiner(",");
        StringJoiner sqlValues = new StringJoiner(",");

        if(table == Constants.getRawDataTable()){
            sql = "INSERT INTO `" + table + "` (";

            for (Map.Entry<String, String> entry : aircraftData.entrySet()) {
                sqlRowsWithValues.add(" `" + entry.getKey() + "`");
                sqlValues.add("'" + entry.getValue() + "'");
            }
            statement.addBatch (sql + sqlRowsWithValues.toString() + ")" + " VALUES( " + sqlValues.toString() + ")");
        }
        if(table == Constants.getCleanDataTable()){
            String query = "";
            if (updateRecord) {
                sql = "DELETE FROM `" + Constants.getCleanDataTable() + "` WHERE ICAO = '" + aircraftData.get("ICAO") + "' AND Time > '" + toLocalDateTime((LocalDateTime.parse(aircraftData.get("Time"), DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss")).toEpochSecond(ZoneOffset.UTC)) - timePlaneStillAlive) + "'";
                statement.addBatch(sql);
                //updateRecord = false;
            }
            sql = "INSERT INTO `" + Constants.getCleanDataTable() + "` (";
            for (Map.Entry<String, String> entry : aircraftData.entrySet()) {
                sqlRowsWithValues.add(" `" + entry.getKey() + "`");
                sqlValues.add("'" + entry.getValue() + "'");
            }
            updateRecord = false;

            statement.addBatch(sql + sqlRowsWithValues.toString() + ")" + " VALUES( " + sqlValues.toString() + ")");
        }
    }

    static void addRecordToDBQueue(HashMap<String, String> aircraftData) throws SQLException {
        try {
            populateSQLQuery(aircraftData, Constants.getRawDataTable());
            if (writeToCleanDataTable(aircraftData, recentPlanes)) {
                populateSQLQuery(aircraftData, Constants.getCleanDataTable());
            }
        }
        catch (SQLException e) {
            System.out.println(e.getMessage());
            System.out.println(LocalDateTime.now());
            System.out.println("DB4");
            Email.sendErrorEmail(e);
        }
    }

    static void writeQueuedRecordsToDB(Statement stmt) throws SQLException {
        try {
            stmt.executeBatch();
        }
        catch (Exception e) {
            System.out.println(e.getMessage());
            System.out.println(LocalDateTime.now());
            e.printStackTrace();
            System.out.println("DB5");
            Email.sendErrorEmail(e);
        }
        finally {
            if (stmt != null) {
                stmt.close();
            }
        }
    }
}
