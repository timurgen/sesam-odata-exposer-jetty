package io.sesam.odata.infrastructure;

import io.sesam.odata.infrastructure.models.Dataset;
import io.sesam.odata.infrastructure.models.PipeMetadata;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.List;
import javax.servlet.ServletContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

/**
 *
 * @author 100tsa
 */
public class MetadataProvider {

    private static final Logger LOGGER = LoggerFactory.getLogger(MetadataProvider.class);

    private final ServletContext ctx;

    MetadataProvider(final ServletContext servletContext) {
        this.ctx = servletContext;
    }

    public List<Dataset> getDatasets() {
        byte[] tokenBytes = (byte[]) this.ctx.getAttribute(AppStartListener.SESAM_TOKEN);
        String sesamUrl = (String) this.ctx.getAttribute(AppStartListener.SESAM_BASE_URL);
        
        assert(tokenBytes != null && sesamUrl != null);
        
        RestTemplate restTemplate = new RestTemplateBuilder().setConnectTimeout(30000).setReadTimeout(30000).build();

        HttpHeaders headers = new HttpHeaders();
        headers.add("Authorization", "Bearer ".concat(new String(tokenBytes, Charset.defaultCharset())));
        HttpEntity entity = new HttpEntity(headers);
        ResponseEntity<List<Dataset>> response = restTemplate.exchange(
                String.format("https://%s/api/datasets", sesamUrl),
                HttpMethod.GET,
                entity,
                new ParameterizedTypeReference<List<Dataset>>() {
        });
        List<Dataset> datasetList = response.getBody();
        return datasetList;
    }
    /**
     * 
     * @param pipeId
     * @return
     * @throws IOException 
     */
    public List<PipeMetadata> getPipeMetadata(String pipeId) throws IOException {
        LOGGER.debug("Getting metadata");
        byte[] tokenBytes = (byte[]) this.ctx.getAttribute(AppStartListener.SESAM_TOKEN);
        String sesamUrl = (String) this.ctx.getAttribute(AppStartListener.SESAM_BASE_URL);
        RestTemplate restTemplate = new RestTemplateBuilder().setConnectTimeout(30000).setReadTimeout(30000).build();

        HttpHeaders headers = new HttpHeaders();
        headers.add("Authorization", "Bearer ".concat(new String(tokenBytes, Charset.defaultCharset())));
        HttpEntity entity = new HttpEntity(headers);
        try {
            ResponseEntity<List<PipeMetadata>> response = restTemplate.exchange(
                    String.format("https://%s/api/pipes/%s/generate-schema-definition?sample_size=50", sesamUrl, pipeId),
                    HttpMethod.GET,
                    entity,
                    new ParameterizedTypeReference<List<PipeMetadata>>() {
            });
            List<PipeMetadata> metadataObjectList = response.getBody();
            return metadataObjectList;
        } catch (RestClientException ex) {
            LOGGER.warn("Couldn't retrieve metadata for pipe {}. Reason: {}", pipeId, ex.getMessage());
            return Collections.EMPTY_LIST;
        }
    }
}
