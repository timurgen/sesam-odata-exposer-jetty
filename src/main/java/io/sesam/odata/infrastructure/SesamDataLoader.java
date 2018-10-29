package io.sesam.odata.infrastructure;

import com.fasterxml.jackson.databind.JsonNode;
import java.nio.charset.Charset;
import java.util.List;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

/**
 * Class used to getting data from sesam
 *
 * @author 100tsa
 */
public class SesamDataLoader {

    private final String url;
    private final byte[] jwt;

    public SesamDataLoader(final String url, final byte[] jwt) {
        this.url = url;
        this.jwt = jwt;
    }

    /**
     * get data from given dataset in Sesam appliance
     *
     * @param datasetId
     * @return
     */
    public final List<JsonNode> getData(final String datasetId, int skiptoken, int elements) {
        final RestTemplate restTemplate = new RestTemplateBuilder().build();

        final HttpHeaders headers = new HttpHeaders();
        headers.add("Authorization", "Bearer ".concat(new String(this.jwt, Charset.defaultCharset())));
        final HttpEntity entity = new HttpEntity(headers);
        ResponseEntity<List<JsonNode>> response = restTemplate.exchange(
                String.format("https://%s/api/datasets/%s/entities?deleted=false&limit=%d&since=%d",
                        this.url, datasetId, elements, skiptoken),
                HttpMethod.GET,
                entity,
                new ParameterizedTypeReference<List<JsonNode>>() {
        });
        final List<JsonNode> datasetList = response.getBody();
        return datasetList;
    }

    /**
     * Return entity with given id from dataset with given id
     *
     * @param datasetId
     * @param entityId
     * @return
     */
    public final JsonNode getEntity(final String datasetId, final String entityId) {
        final RestTemplate restTemplate = new RestTemplateBuilder().build();

        final HttpHeaders headers = new HttpHeaders();
        headers.add("Authorization", "Bearer ".concat(new String(this.jwt, Charset.defaultCharset())));
        headers.add("Accept", "application/json");
        final HttpEntity entity = new HttpEntity(headers);
        ResponseEntity<JsonNode> response = restTemplate.exchange(
                String.format("https://%s/api/datasets/%s/entity?entity_id=%s",
                        this.url, datasetId, entityId),
                HttpMethod.GET,
                entity,
                new ParameterizedTypeReference<JsonNode>() {
        });
        final JsonNode resultEntity = response.getBody();
        return resultEntity;
    }
}
