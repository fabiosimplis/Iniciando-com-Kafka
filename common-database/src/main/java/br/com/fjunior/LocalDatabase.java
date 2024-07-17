package br.com.fjunior;

import java.sql.*;

public class LocalDatabase {

    private final Connection connection;

    public LocalDatabase(String name) throws SQLException {
        String url = "jdbc:sqlite:target/"+name+".db";
        connection = DriverManager.getConnection(url);
    }

    // yes, this is way too generic
    // according to your database tool, avoid injection
    public void createIfNotExists(String sql){
        try {
            connection.createStatement().execute(sql);
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
    }

    public void update(String statement, String ... params) throws SQLException {
        var preparedStatement = prepare(statement, params);
        preparedStatement.execute();
    }

    public ResultSet query(String query, String... params) throws SQLException {
        var preparedStatement = prepare(query, params);
        return preparedStatement.executeQuery();
    }

    private PreparedStatement prepare(String query, String[] params) throws SQLException {
        var preparedStatement = connection.prepareStatement(query);
        for (int i = 0; i < params.length ; i++) {
            preparedStatement.setString(i + 1, params[i]);
        }
        return preparedStatement;
    }

    public void close() throws SQLException {
        connection.close();
    }
}
