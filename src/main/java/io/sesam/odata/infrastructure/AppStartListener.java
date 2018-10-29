package io.sesam.odata.infrastructure;

import io.sesam.odata.edm.SesamEdmProvider;
import io.sesam.odata.infrastructure.models.Dataset;
import io.sesam.odata.infrastructure.models.PipeMetadata;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Web application lifecycle listener.
 *
 * @author 100tsa
 */
public class AppStartListener implements ServletContextListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(AppStartListener.class);
    //env variables which must be provided to connect to sesam appliance
    /**
     * Environmental variable which must containt url to Sesam instance
     */
    public static final String SESAM_BASE_URL = "SESAM_URL";
    /**
     * Environmental variable which must contain JWT token for connecting to sesam appliance
     */
    public static final String SESAM_TOKEN = "SESAM_JWT";

    public static final String SESAM_BASE_API_PATH = "api/";

    private String sesamBaseUrl;



    @Override
    public void contextInitialized(ServletContextEvent sce) {
        ServletContext servletContext = sce.getServletContext();

        if (null == System.getenv(SESAM_BASE_URL) || null == System.getenv(SESAM_TOKEN)) {
            printErrorAndExit();
        }

        this.sesamBaseUrl = System.getenv(SESAM_BASE_URL);
        byte[] jwtToken = System.getenv(SESAM_TOKEN).getBytes(Charset.defaultCharset());

        servletContext.setAttribute(SESAM_TOKEN, jwtToken);
        servletContext.setAttribute(SESAM_BASE_URL, this.sesamBaseUrl);

        LOGGER.info("Service started: ");
        LOGGER.info("Getting metadata from Sesam appliance");

        MetadataProvider mProvider = new MetadataProvider(sce.getServletContext());
        List<Dataset> datasets = mProvider.getDatasets();

        datasets.stream().filter((t) -> {
            return "user".equals(t.runtime.getOrigin());
        }).forEach((Dataset t) -> {
            try {
                List<PipeMetadata> pipeMetadata = mProvider.getPipeMetadata(t.getId());

                if (pipeMetadata.isEmpty()) {
                    return;
                }

                //convert hyphen strings to camelCase as OData don't like dashes in URI
                String camelCasedId = Arrays.stream(t.getId().split("\\-"))
                        .map(String::toLowerCase)
                        .map(s -> s.substring(0, 1).toUpperCase() + s.substring(1))
                        .collect(Collectors.joining());

                //removing namespaces as : is not allowed in odata names and will cause serialization exception
                pipeMetadata.replaceAll((PipeMetadata p) -> {
                    String[] splitedName = p.getName().split(":");
                    p.setName(splitedName[splitedName.length - 1]);
                    return p;
                });
                //add _id property as it exixts but not in metadata
                PipeMetadata _idObj = new PipeMetadata();
                _idObj.setName("_id");
                _idObj.setType("string");
                pipeMetadata.add(_idObj);
                SesamEdmProvider.registerEdmType(camelCasedId, pipeMetadata);
                SesamEdmProvider.registerEdmReference(camelCasedId, t);
            } catch (IOException ex) {
                LOGGER.warn("Couldn't get meatadta for pipe {}. Reason: {}", t.getId(), ex.getMessage());
            }
        });
    }

    /**
     * Outputs startup error in log and exit applicaiton by throwing Runtime exception
     *
     * @throws RuntimeException
     */
    private void printErrorAndExit(){
        LOGGER.error("{} and {} env vars must be provided", SESAM_BASE_URL, SESAM_TOKEN);
        throw new RuntimeException("Invalid configuration.");
    }

    @Override
    public void contextDestroyed(ServletContextEvent sce) {
        LOGGER.info("Service stopped");
    }

    @Override
    public String toString() {
        return "AppStartListener{" + "sesamBaseUrl=" + sesamBaseUrl + '}';
    }
    
}
