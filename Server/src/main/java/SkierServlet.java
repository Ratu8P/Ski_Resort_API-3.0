import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

@WebServlet(value = "/skiers/*")

public class SkierServlet extends HttpServlet {

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse res) throws ServletException, IOException {
        res.setContentType("text/plain");
        String urlPath = req.getPathInfo();

        // check if the url exist
        if (urlPath == null || urlPath.isEmpty()) {
            res.setStatus(HttpServletResponse.SC_NOT_FOUND);
            res.getWriter().write("Missing parameters");
            return;
        }

        String[] urlParts = urlPath.split("/");

        // validate url
        if (!isUrlValid(urlParts)) {
            res.setStatus(HttpServletResponse.SC_NOT_FOUND);
            res.getWriter().write("Invalid URL");
        } else {
            res.setStatus(HttpServletResponse.SC_OK);
            res.getWriter().write("It works! GET request handled successfully.");
        }
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse res) throws ServletException, IOException {
        // handle POST
        res.setContentType("application/json");
        res.setStatus(HttpServletResponse.SC_CREATED);
        res.getWriter().write("{ \"status\": \"Lift ride recorded successfully.\" }");
    }

    // 验证 URL 的方法
    private boolean isUrlValid(String[] urlPath) {
        // example /skiers/{resortID}/seasons/{seasonID}/days/{dayID}/skiers/{skierID}
        if (urlPath.length == 8) {
            // [ , resortID, seasons, seasonID, days, dayID, skiers, skierID ]
            try {
                Integer.parseInt(urlPath[1]); // resortID
                Integer.parseInt(urlPath[3]); // seasonID
                Integer.parseInt(urlPath[5]); // dayID
                Integer.parseInt(urlPath[7]); // skierID
                return true;
            } catch (NumberFormatException e) {
                return false;
            }
        }
        return false;
    }
}
