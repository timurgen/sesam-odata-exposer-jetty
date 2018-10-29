package io.sesam.odata.controller;

import io.sesam.odata.edm.SesamEdmProvider;
import io.sesam.odata.service.ServiceProcessor;
import io.sesam.odata.service.SesamEntityCollectionProcessor;
import io.sesam.odata.service.SesamEntityProcessor;
import java.io.IOException;
import java.util.ArrayList;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.olingo.server.api.OData;
import org.apache.olingo.server.api.ODataHttpHandler;
import org.apache.olingo.server.api.ServiceMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main entry point mapped to all sesam.svc/* HTTP requests
 *
 * @author 100tsa
 */
public class SesamOdataController extends HttpServlet {

    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = LoggerFactory.getLogger(SesamOdataController.class);

    @Override
    protected void service(final HttpServletRequest req, final HttpServletResponse resp)
            throws ServletException, IOException {
        try {
            LOGGER.debug("Serving request {} from {}", req.getRequestURI(), req.getRemoteAddr());
            
            OData odata = OData.newInstance();
            ServiceMetadata edm = odata.createServiceMetadata(new SesamEdmProvider(), new ArrayList<>());
            ODataHttpHandler handler = odata.createHandler(edm);
            
            handler.register(new SesamEntityCollectionProcessor(getServletContext()));
            handler.register(new SesamEntityProcessor(getServletContext()));
            handler.register(new ServiceProcessor());
            
            handler.process(req, resp);
        } catch (RuntimeException e) {
            LOGGER.error("Server Error occurred in SesamOdataController", e);
            throw new ServletException(e);
        }
    }
}
