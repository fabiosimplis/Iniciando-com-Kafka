package br.com.fjunior.ecommerce;

import br.com.fjunior.LocalDatabase;

import java.io.Closeable;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;

public class OrdersDataBase implements Closeable {

    private final LocalDatabase database;

    OrdersDataBase() throws SQLException {
        this.database = new LocalDatabase("orders_database");
        //you might want to save all data
        this.database.createIfNotExists("CREATE TABLE Orders (" +
                "uuid varchar(200) primary key)");
    }

    public boolean saveNew(Order order) throws SQLException {
        if (wasProcessed(order)){
            return false;
        }
        database.update("INSERT INTO ORDERS (uuid) VALUES (?)", order.getOrderId());
        return true;
    }

    private boolean wasProcessed(Order order) throws SQLException {
        ResultSet results = database.query("SELECT uuid FROM Orders WHERE uuid = ? LIMIT 1", order.getOrderId());
        return results.next();
    }

    @Override
    public void close() throws IOException {
        try {
            database.close();
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }
}
