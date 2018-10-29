package io.sesam.odata.service;

import com.fasterxml.jackson.databind.JsonNode;
import io.sesam.odata.edm.SesamEdmProvider;
import io.sesam.odata.infrastructure.AppStartListener;
import io.sesam.odata.infrastructure.SesamDataLoader;
import io.sesam.odata.infrastructure.models.Dataset;
import io.sesam.odata.infrastructure.models.PipeMetadata;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import javax.servlet.ServletContext;
import org.apache.olingo.commons.api.data.ContextURL;
import org.apache.olingo.commons.api.data.Entity;
import org.apache.olingo.commons.api.data.EntityCollection;
import org.apache.olingo.commons.api.data.Property;
import org.apache.olingo.commons.api.data.ValueType;
import org.apache.olingo.commons.api.edm.EdmEntitySet;
import org.apache.olingo.commons.api.edm.EdmEntityType;
import org.apache.olingo.commons.api.ex.ODataRuntimeException;
import org.apache.olingo.commons.api.format.ContentType;
import org.apache.olingo.commons.api.http.HttpHeader;
import org.apache.olingo.commons.api.http.HttpStatusCode;
import org.apache.olingo.server.api.OData;
import org.apache.olingo.server.api.ODataApplicationException;
import org.apache.olingo.server.api.ODataLibraryException;
import org.apache.olingo.server.api.ODataRequest;
import org.apache.olingo.server.api.ODataResponse;
import org.apache.olingo.server.api.ServiceMetadata;
import org.apache.olingo.server.api.processor.EntityCollectionProcessor;
import org.apache.olingo.server.api.serializer.EntityCollectionSerializerOptions;
import org.apache.olingo.server.api.serializer.ODataSerializer;
import org.apache.olingo.server.api.serializer.SerializerResult;
import org.apache.olingo.server.api.uri.UriInfo;
import org.apache.olingo.server.api.uri.UriResource;
import org.apache.olingo.server.api.uri.UriResourceEntitySet;
import org.apache.olingo.server.api.uri.queryoption.SkipOption;
import org.apache.olingo.server.api.uri.queryoption.TopOption;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.sesam.odata.edm.SesamEdmProvider.SET_POSTFIX;
import java.util.Locale;
import org.apache.olingo.server.api.uri.queryoption.SkipTokenOption;
import org.apache.olingo.server.api.uri.queryoption.SystemQueryOptionKind;

/**
 *
 * @author 100tsa
 */
public class SesamEntityCollectionProcessor implements EntityCollectionProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(SesamEntityCollectionProcessor.class);
    private static final int PAGE_SIZE = 100;

    private OData odata;
    private ServiceMetadata serviceMetadata;
    private final ServletContext ctx;

    public SesamEntityCollectionProcessor(ServletContext servletContext) {
        this.ctx = servletContext;
    }

    @Override
    public void readEntityCollection(ODataRequest req, ODataResponse res, UriInfo uI, ContentType cT)
            throws ODataApplicationException, ODataLibraryException {

        try {
            // 1st we have retrieve the requested EntitySet from the uriInfo object (representation of the parsed service URI)
            List<UriResource> resourcePaths = uI.getUriResourceParts();
            // in our situation, the first segment is the EntitySet
            UriResourceEntitySet uriResourceEntitySet = (UriResourceEntitySet) resourcePaths.get(0);

            //checking if skip/top options presented
            int skipNumber = 0;

            SkipOption skipOption = uI.getSkipOption();
            if (skipOption != null) {
                skipNumber = skipOption.getValue();
            }

            int topNumber = -1;
            TopOption topOption = uI.getTopOption();
            if (topOption != null) {
                topNumber = topOption.getValue();
            }

            int skiptoken = 0;
            SkipTokenOption skipTokenOption = uI.getSkipTokenOption();
            if (null != skipTokenOption) {
                skiptoken = Integer.valueOf(skipTokenOption.getValue());
            }

            EdmEntitySet edmEntitySet = uriResourceEntitySet.getEntitySet();

            LOGGER.debug("Requesting {} entity set", edmEntitySet.getName());

            // 2nd: fetch the data from backend for this requested EntitySetName
            // it has to be delivered as EntitySet object
            EntityCollection entitySet = getData(edmEntitySet, skipNumber, topNumber, skiptoken);

            if (entitySet.getEntities().size() == PAGE_SIZE) {
                entitySet.setNext(createNextLink(req.getRawRequestUri(), skiptoken + PAGE_SIZE));
            }

            // 3rd: create a serializer based on the requested format (json)
            ODataSerializer serializer = this.odata.createSerializer(cT);

            // 4th: Now serialize the content: transform from the EntitySet object to InputStream
            EdmEntityType edmEntityType = edmEntitySet.getEntityType();
            ContextURL contextUrl = ContextURL.with()
                    .entitySet(edmEntitySet)
                    .serviceRoot(new URI(req.getRawBaseUri() + "/"))
                    .build();

            final String id = req.getRawBaseUri() + "/" + edmEntitySet.getName();
            EntityCollectionSerializerOptions opts = EntityCollectionSerializerOptions.with().id(id)
                    .contextURL(contextUrl).build();
            SerializerResult serRes = serializer.entityCollection(this.serviceMetadata, edmEntityType, entitySet, opts);
            InputStream serializedContent = serRes.getContent();

            // Finally: configure the response object: set the body, headers and status code
            res.setContent(serializedContent);
            res.setStatusCode(HttpStatusCode.OK.getStatusCode());
            res.setHeader(HttpHeader.CONTENT_TYPE, cT.toContentTypeString());
        } catch (URISyntaxException ex) {
            LOGGER.error("Can't construct URI object : {}", ex.getMessage());
        }
    }

    @Override
    public void init(OData odata, ServiceMetadata serviceMetadata) {
        LOGGER.debug("Initializing oData collections processor class {}", getClass().getName());
        this.odata = odata;
        this.serviceMetadata = serviceMetadata;
    }

    /**
     *
     * @param edmEntitySet
     *
     * @return
     */
    private EntityCollection getData(EdmEntitySet edmEntitySet, int skipNumber, int topNumber, int skiptoken) {
        //TODO addsupport for skipNumber & topNumber
        EntityCollection entityCollection = new EntityCollection();
        List<Entity> entities = entityCollection.getEntities();
        String edmSetName = edmEntitySet.getName();
        if (edmSetName.endsWith(SET_POSTFIX)) {
            edmSetName = edmSetName.replace(SET_POSTFIX, "");
        }
        if (!SesamEdmProvider.getEdmMap().containsKey(edmSetName)) {
            return entityCollection;
        }

        Dataset dataset = SesamEdmProvider.getEdmRefMap().get(edmSetName);
        List<PipeMetadata> metadata = SesamEdmProvider.getEdmMap().get(edmSetName);

        SesamDataLoader dataSource = new SesamDataLoader(
                (String) this.ctx.getAttribute(AppStartListener.SESAM_BASE_URL),
                (byte[]) this.ctx.getAttribute(AppStartListener.SESAM_TOKEN));

        List<JsonNode> data = dataSource.getData(dataset.getId(), skiptoken, PAGE_SIZE);
        data.forEach((jsonNode) -> {
            Entity entity = new Entity();
            jsonNode.fields().forEachRemaining((entry) -> {
                String key = entry.getKey();

                metadata.forEach((metadataObj) -> {
                    String[] splitedKey = key.split(":");
                    String entityValue = entry.getValue().asText();
                    if (metadataObj.name.equals(key) || metadataObj.name.equals(splitedKey[splitedKey.length - 1])) {
                        Object entityValueObj = this.resolveEntityType(metadataObj.type, entityValue);
                        if (key.contains(":")) {
                            String[] splitedName = key.split(":");
                            entity.addProperty(new Property(null, splitedName[1], ValueType.PRIMITIVE, entityValueObj));
                        } else {
                            entity.addProperty(new Property(null, key, ValueType.PRIMITIVE, entityValueObj));
                        }

                    }
                });

            });
            String id = jsonNode.get("_id").asText();
            //ensure tyhat id do not have namespace as colon is illegal as part of URI
            //but quoted string seems to have it allowed
            if (null != id && id.contains(":")) {
                id = id.split(":")[1];
            }
            entity.setId(createId(edmEntitySet.getName(), id));
            entities.add(entity);
        });

        return entityCollection;
    }

    private URI createId(String entitySetName, Object id) {
        try {
            //FIXME as all sesam keys are strings we put '', but in case of numeric keys
            //we need to provide qoutesignfree implementation
            return new URI(entitySetName + "('" + String.valueOf(id) + "')");
        } catch (URISyntaxException e) {
            LOGGER.error("Unable to create id {} for entity: {}", id, entitySetName, e);
            throw new ODataRuntimeException("Unable to create id for entity: " + entitySetName, e);
        }
    }

    private Object resolveEntityType(String type, String entityValue) {
        switch (type) {
            case "string":
                return entityValue;
            case "integer":
                return Long.valueOf(entityValue);
            case "datetime":
                return entityValue.replace("~t", "");
        }
        //fallback to string as default
        return entityValue;
    }

    private static URI createNextLink(final String rawRequestUri, final int skipToken)
            throws ODataApplicationException {
        // Remove a maybe existing skiptoken, making sure that the query part is not empty.
        String nextlink = rawRequestUri.contains("?")
                ? rawRequestUri.replaceAll("(\\$|%24)skiptoken=.+&?", "").replaceAll("(\\?|&)$", "")
                : rawRequestUri;

        // Add a question mark or an ampersand, depending on the current query part.
        nextlink += nextlink.contains("?") ? '&' : '?';

        // Append the new skiptoken.
        nextlink += SystemQueryOptionKind.SKIPTOKEN.toString() + '=' + skipToken;
        try {
            return new URI(nextlink);
        } catch (final URISyntaxException e) {
            throw new ODataApplicationException("Exception while constructing next link",
                    HttpStatusCode.INTERNAL_SERVER_ERROR.getStatusCode(), Locale.ROOT, e);
        }
    }

}
