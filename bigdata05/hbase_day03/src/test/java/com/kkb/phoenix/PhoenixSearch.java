package com.kkb.phoenix;

import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.sql.*;

public class PhoenixSearch {
    private Connection connection;
    private Statement statement;
    private ResultSet rs;

    @BeforeTest
    public void init() throws SQLException {
        String url = "jdbc:phoenix:node1:2181";
        connection = DriverManager.getConnection(url);
        statement = connection.createStatement();
    }

    @Test
    public void queryTable() throws SQLException {
        String sql = "SELECT * FROM US_POPULATION";
        try {
            rs = statement.executeQuery(sql);
            while (rs.next()) {
                System.out.println("state: " + rs.getString("state"));
                System.out.println("city: " + rs.getString("city"));
                System.out.println("population: " + rs.getString("population"));
                System.out.println("-------------------");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }
}
