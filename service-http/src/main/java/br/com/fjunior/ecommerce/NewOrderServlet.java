package br.com.fjunior.ecommerce;

import br.com.fjunior.ecommerce.dispacher.KafkaDispatcher;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderServlet extends HttpServlet {

    private final KafkaDispatcher<Order> orderDispacher = new KafkaDispatcher<>();

    @Override
    public void destroy() {
        super.destroy();
        orderDispacher.close();
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {

        try {
            //localhost:8080/new?email=guilherme@email.com&amount=153
            // I'm not caring about any security issues, we are only
            var email = req.getParameter("email");
            //var orderId = UUID.randomUUID().toString();
            var orderId = req.getParameter("uuid");
            var amount = new BigDecimal(req.getParameter("amount"));
            var order = new Order(orderId, amount, email);

            try (var database = new OrdersDataBase()) {

                if (database.saveNew(order)) {

                    orderDispacher.send("ECOMMERCE_NEW_ORDER", email, new CorrelationId(NewOrderServlet.class.getSimpleName()), order);

                    System.out.println("New order sent successfully");
                    resp.setStatus(HttpServletResponse.SC_OK);
                    resp.getWriter().println("New order sent successfully");
                } else {
                    System.out.println("Order already received");
                    resp.setStatus(HttpServletResponse.SC_CONFLICT);
                    resp.getWriter().println("Order already received");
                }
            }
        } catch (ExecutionException | InterruptedException | SQLException e) {
            throw new ServletException(e);
        }

    }
}
