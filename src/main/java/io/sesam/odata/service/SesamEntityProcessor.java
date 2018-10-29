package io.sesam.odata.service;

import com.fasterxml.jackson.databind.JsonNode;
import io.sesam.odata.edm.SesamEdmProvider;
import static io.sesam.odata.edm.SesamEdmProvider.SET_POSTFIX;
import io.sesam.odata.infrastructure.AppStartListener;
import io.sesam.odata.infrastructure.SesamDataLoader;
import io.sesam.odata.infrastructure.models.Dataset;
import io.sesam.odata.infrastructure.models.PipeMetadata;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Locale;
import javax.servlet.ServletContext;
import org.apache.olingo.commons.api.data.ContextURL;
import org.apache.olingo.commons.api.data.Entity;
import org.apache.olingo.commons.api.data.Property;
import org.apache.olingo.commons.api.data.ValueType;
import org.apache.olingo.commons.api.edm.EdmEntitySet;
import org.apache.olingo.commons.api.edm.EdmEntityType;
import org.apache.olingo.commons.api.format.ContentType;
import org.apache.olingo.commons.api.http.HttpHeader;
import org.apache.olingo.commons.api.http.HttpStatusCode;
import org.apache.olingo.server.api.OData;
import org.apache.olingo.server.api.ODataApplicationException;
import org.apache.olingo.server.api.ODataLibraryException;
import org.apache.olingo.server.api.ODataRequest;
import org.apache.olingo.server.api.ODataResponse;
import org.apache.olingo.server.api.ServiceMetadata;
import org.apache.olingo.server.api.processor.EntityProcessor;
import org.apache.olingo.server.api.serializer.EntitySerializerOptions;
import org.apache.olingo.server.api.serializer.ODataSerializer;
import org.apache.olingo.server.api.serializer.SerializerResult;
import org.apache.olingo.server.api.uri.UriInfo;
import org.apache.olingo.server.api.uri.UriParameter;
import org.apache.olingo.server.api.uri.UriResource;
import org.apache.olingo.server.api.uri.UriResourceEntitySet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.olingo.commons.api.http.HttpStatusCode.NOT_IMPLEMENTED;

/**
 *
 * @author 100tsa
 */
public class SesamEntityProcessor implements EntityProcessor {

    private final static Logger LOGGER = LoggerFactory.getLogger(SesamEntityProcessor.class);

    private OData odata;
    private ServiceMetadata metadata;
    private final ServletContext ctx;

    public SesamEntityProcessor(ServletContext ctx) {
        this.ctx = ctx;
    }

    @Override
    public void readEntity(ODataRequest req, ODataResponse res, UriInfo uI, ContentType cT)
            throws ODataApplicationException, ODataLibraryException {
        try {
            // 1. retrieve the Entity Type
            List<UriResource> resourcePaths = uI.getUriResourceParts();
            // Note: only in our example we can assume that the first segment is the EntitySet
            UriResourceEntitySet uriResourceEntitySet = (UriResourceEntitySet) resourcePaths.get(0);
            EdmEntitySet edmEntitySet = uriResourceEntitySet.getEntitySet();

            // 2. retrieve the data from backend
            List<UriParameter> keyPredicates = uriResourceEntitySet.getKeyPredicates();
            Entity entity = getData(edmEntitySet, keyPredicates);

            // 3. serialize
            EdmEntityType entityType = edmEntitySet.getEntityType();

            ContextURL contextUrl = ContextURL
                    .with()
                    .entitySet(edmEntitySet)
                    .serviceRoot(new URI(req.getRawBaseUri() + "/"))
                    .build();
            // expand and select currently not supported
            EntitySerializerOptions options = EntitySerializerOptions.with().contextURL(contextUrl).build();

            ODataSerializer serializer = odata.createSerializer(cT);
            SerializerResult serializerResult = serializer.entity(this.metadata, entityType, entity, options);
            InputStream entityStream = serializerResult.getContent();

            //4. configure the response object
            res.setContent(entityStream);
            res.setStatusCode(HttpStatusCode.OK.getStatusCode());
            res.setHeader(HttpHeader.CONTENT_TYPE, cT.toContentTypeString());
        } catch (URISyntaxException ex) {
            LOGGER.error("Couldn't construct id URI ", ex);
        }
    }

    @Override
    public void createEntity(ODataRequest req, ODataResponse res, UriInfo uI, ContentType cT, ContentType ct2)
            throws ODataApplicationException, ODataLibraryException {
        throw new ODataApplicationException("Not supported yet.", NOT_IMPLEMENTED.getStatusCode(), Locale.ROOT);
    }

    @Override
    public void updateEntity(ODataRequest req, ODataResponse res, UriInfo uI, ContentType ct, ContentType ct2)
            throws ODataApplicationException, ODataLibraryException {
        throw new ODataApplicationException("Not supported yet.", NOT_IMPLEMENTED.getStatusCode(), Locale.ROOT);
    }

    @Override
    public void deleteEntity(ODataRequest req, ODataResponse res, UriInfo uI)
            throws ODataApplicationException, ODataLibraryException {
        throw new ODataApplicationException("Not supported yet.", NOT_IMPLEMENTED.getStatusCode(), Locale.ROOT);
    }

    @Override
    public void init(OData odata, ServiceMetadata serviceMetadata) {
        LOGGER.trace("Initializing oData entity processoe class {}", getClass().getName());
        this.odata = odata;
        this.metadata = serviceMetadata;
    }

    private Entity getData(EdmEntitySet edmEntitySet, List<UriParameter> keyPredicates) {
        EdmEntityType edmEntityType = edmEntitySet.getEntityType();
        String edmTypeName = edmEntityType.getName();
        String entityId = keyPredicates.get(0).getText().replace("'", "");

        String edmSetName = edmEntitySet.getName();
        if (edmSetName.endsWith(SET_POSTFIX)) {
            edmSetName = edmSetName.replace(SET_POSTFIX, "");
        }
        if (!SesamEdmProvider.getEdmMap().containsKey(edmSetName)) {
            return null;
        }

        Dataset dataset = SesamEdmProvider.getEdmRefMap().get(edmSetName);
        List<PipeMetadata> pipeMetadata = SesamEdmProvider.getEdmMap().get(edmSetName);

        SesamDataLoader dataSource = new SesamDataLoader(
                (String) this.ctx.getAttribute(AppStartListener.SESAM_BASE_URL),
                (byte[]) this.ctx.getAttribute(AppStartListener.SESAM_TOKEN));

        String namespacedId = dataset.getId() + ":" + entityId;
        JsonNode entity = dataSource.getEntity(dataset.getId(), namespacedId);
        Entity resultEntity = new Entity();

        entity.fields().forEachRemaining((entry) -> {
            String key = entry.getKey();

            pipeMetadata.forEach((metadataObj) -> {
                String[] splitedKey = key.split(":");
                String entityValue = entry.getValue().asText();
                if (metadataObj.name.equals(key) || metadataObj.name.equals(splitedKey[splitedKey.length - 1])) {
                    Object entityValueObj = this.resolveEntityType(metadataObj.type, entityValue);
                    if (key.contains(":")) {
                        String[] splitedName = key.split(":");
                        resultEntity.addProperty(new Property(null, splitedName[1], ValueType.PRIMITIVE, entityValueObj));
                    } else {
                        resultEntity.addProperty(new Property(null, key, ValueType.PRIMITIVE, entityValueObj));
                    }

                }
            });
        });

        return resultEntity;

    }

    private Object resolveEntityType(String type, String entityValue) {
        switch (type) {
            case "string":
                return entityValue;
            case "integer":
                return Long.valueOf(entityValue);
            case "datetime":
                return entityValue;
        }
        //fallback to string as default
        return entityValue;
    }

}
