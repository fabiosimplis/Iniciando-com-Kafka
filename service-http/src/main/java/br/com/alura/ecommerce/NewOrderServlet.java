package br.com.alura.ecommerce;

import br.com.fjunior.ecommerce.KafkaDispatcher;
import jakarta.servlet.ServletConfig;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderServlet extends HttpServlet {

    private final KafkaDispatcher<Order> orderDispacher = new KafkaDispatcher<>();
    private final KafkaDispatcher<Email> emailDispacher = new KafkaDispatcher<>();


    @Override
    public void destroy() {
        super.destroy();
        orderDispacher.close();
        emailDispacher.close();
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {

        try {
            //localhost:8080/new?email=guilherme@email.com&amount=153
            // I'm not caring about any security issues, we are only
            var email = req.getParameter("email");
            var orderId = UUID.randomUUID().toString();
            var amount = new BigDecimal(req.getParameter("amount"));
            var order = new Order(orderId, amount, email);
            orderDispacher.send("ECOMMERCE_NEW_ORDER", email, order);

            Email emailMessage = new Email("Ecommerce","Thank you for your order " + orderId + "!\nWe are processing your order!");
            emailDispacher.send("ECOMMERCE_SEND_EMAIL", email, emailMessage);

            System.out.println("New order sent successfully");
            resp.setStatus(HttpServletResponse.SC_OK);
            resp.getWriter().println("New order sent successfully");

        } catch (ExecutionException e) {
            throw new ServletException(e);
        } catch (InterruptedException e) {
            throw new ServletException(e);
        }

    }
}
