package io.sesam.odata.edm;

import io.sesam.odata.infrastructure.models.Dataset;
import io.sesam.odata.infrastructure.models.PipeMetadata;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.apache.olingo.commons.api.edm.provider.CsdlAbstractEdmProvider;
import org.apache.olingo.commons.api.edm.provider.CsdlEntityContainer;
import org.apache.olingo.commons.api.edm.provider.CsdlEntityContainerInfo;
import org.apache.olingo.commons.api.edm.provider.CsdlEntitySet;
import org.apache.olingo.commons.api.edm.provider.CsdlEntityType;
import org.apache.olingo.commons.api.edm.provider.CsdlProperty;
import org.apache.olingo.commons.api.edm.provider.CsdlPropertyRef;
import org.apache.olingo.commons.api.edm.provider.CsdlSchema;
import org.apache.olingo.commons.api.ex.ODataException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides information about available datasets and their structure
 *
 * @author 100tsa
 */
public class SesamEdmProvider extends CsdlAbstractEdmProvider {

    private static final Logger LOGGER = LoggerFactory.getLogger(SesamEdmProvider.class);
    private static final Map<String, List<PipeMetadata>> EDM_MAP = new HashMap<>(16);
    private static final Map<String, Dataset> EDM_REF_MAP = new HashMap<>(16);
    public static final String SET_POSTFIX = "_set";
    public static final String ID_PROPERTY = "_id";

    // Service Namespace
    public static final String NAMESPACE = "Sesam.io";

    // EDM Container
    public static final String CONTAINER_NAME = "Odata";
    public static final FullQualifiedName CONTAINER = new FullQualifiedName(NAMESPACE, CONTAINER_NAME);

    public static void registerEdmReference(String camelCasedId, Dataset t) {
        SesamEdmProvider.EDM_REF_MAP.put(camelCasedId, t);
    }

    /**
     *
     * @return @throws ODataException
     */
    @Override
    public CsdlEntityContainer getEntityContainer() throws ODataException {
        // create EntitySets
        List<CsdlEntitySet> entitySets = new ArrayList<>();
        EDM_MAP.forEach((k, v) -> {
            try {
                entitySets.add(getEntitySet(CONTAINER, k));
            } catch (ODataException ex) {
                LOGGER.warn("Couldn't get entity set data: {}", ex.getMessage());
            }
        });

        // create EntityContainer
        CsdlEntityContainer entityContainer = new CsdlEntityContainer();
        entityContainer.setName(CONTAINER_NAME);
        entityContainer.setEntitySets(entitySets);

        return entityContainer;
    }

    /**
     *
     * @return @throws ODataException
     */
    @Override
    public List<CsdlSchema> getSchemas() throws ODataException {
        // create Schema
        CsdlSchema schema = new CsdlSchema();
        schema.setNamespace(NAMESPACE);

        // add EntityTypes
        List<CsdlEntityType> entityTypes = new ArrayList<>();
        EDM_MAP.forEach((k, v) -> {
            try {
                FullQualifiedName fullQualifiedName = new FullQualifiedName(NAMESPACE, k);
                entityTypes.add(getEntityType(fullQualifiedName));
            } catch (ODataException ex) {
                LOGGER.warn("Error occured while getting entity type {}. Reason {}", k, ex.getMessage());
            }
        });

        schema.setEntityTypes(entityTypes);

        // add EntityContainer
        schema.setEntityContainer(getEntityContainer());

        // finally
        List<CsdlSchema> schemas = new ArrayList<>();
        schemas.add(schema);

        return schemas;
    }

    /**
     *
     * @param entityContainerName
     * @return
     * @throws ODataException
     */
    @Override
    public CsdlEntityContainerInfo getEntityContainerInfo(FullQualifiedName entityContainerName) throws ODataException {
        // This method is invoked when displaying the Service Document at e.g. 
        // http://localhost:8080/sesam.svc/
        if (entityContainerName == null || entityContainerName.equals(CONTAINER)) {
            CsdlEntityContainerInfo entityContainerInfo = new CsdlEntityContainerInfo();
            entityContainerInfo.setContainerName(CONTAINER);
            return entityContainerInfo;
        }
        return null;
    }

    /**
     *
     * @param contnrName
     * @param entitySetName
     * @return
     * @throws ODataException
     */
    @Override
    public CsdlEntitySet getEntitySet(final FullQualifiedName contnrName, final String entitySetName)
            throws ODataException {
        String localEntitySetName = entitySetName;
        //to make difference between Entity name and enity set name we added _set 
        //here we need to remove it t be able to fond entotoes in map
        if (entitySetName.endsWith(SET_POSTFIX)) {
            localEntitySetName = entitySetName.replace(SET_POSTFIX, "");
        }
        if (!contnrName.equals(CONTAINER) || !EDM_MAP.containsKey(localEntitySetName)) {
            return null;
        }
        CsdlEntitySet entitySet = new CsdlEntitySet();
        entitySet.setName(localEntitySetName + SET_POSTFIX);
        entitySet.setType(new FullQualifiedName(NAMESPACE, localEntitySetName));
        return entitySet;
    }

    /**
     *
     * @param entityTypeName
     * @return
     * @throws ODataException
     */
    @Override
    public CsdlEntityType getEntityType(FullQualifiedName entityTypeName) throws ODataException {
        // this method is called for one of the EntityTypes that are configured in the Schema
        if (!EDM_MAP.containsKey(entityTypeName.getName())) {
            return null;
        }
        List<PipeMetadata> metadata = EDM_MAP.get(entityTypeName.getName());
        //create EntityType properties
        List<CsdlProperty> properties = new LinkedList<>();
        //id in sesam datasets is always String _id
        CsdlProperty idProperty = new CsdlProperty()
                .setName(ID_PROPERTY)
                .setType(EdmPrimitiveTypeKind.String.getFullQualifiedName());
        properties.add(idProperty);
        //all other properties fetched from Sesam API
        metadata.forEach((t) -> {
            CsdlProperty property = new CsdlProperty()
                    .setName(t.getName())
                    .setType(resolveEdmType(t.getType()));
            properties.add(property);
        });

        // create CsdlPropertyRef for Key element
        CsdlPropertyRef propertyRef = new CsdlPropertyRef();
        propertyRef.setName(ID_PROPERTY);

        // configure EntityType
        CsdlEntityType entityType = new CsdlEntityType();
        entityType.setName(entityTypeName.getName());
        entityType.setProperties(properties);
        entityType.setKey(Collections.singletonList(propertyRef));

        return entityType;

    }

    /**
     * Adds new metadata obj in EDM type map
     *
     * @param name
     * @param metadata
     * @return false if already registered with the same name (don't added) or true otherwise
     */
    public static boolean registerEdmType(String name, List<PipeMetadata> metadata) {
        if (SesamEdmProvider.EDM_MAP.containsKey(name)) {
            LOGGER.debug("Metadata for {} already exists", name);
            return false;
        }
        LOGGER.info("Register metadata for pipe {}", name);
        SesamEdmProvider.EDM_MAP.put(name, metadata);
        return true;
    }

    /**
     * removes everything from EDM type map
     */
    public static void cleanEdmMap() {
        SesamEdmProvider.EDM_MAP.clear();
    }

    /**
     *
     * @return
     */
    public static Map<String, List<PipeMetadata>> getEdmMap() {
        return SesamEdmProvider.EDM_MAP;
    }

    /**
     *
     * @return
     */
    public static Map<String, Dataset> getEdmRefMap() {
        return SesamEdmProvider.EDM_REF_MAP;
    }

    /**
     *
     * @param type
     * @return
     */
    private FullQualifiedName resolveEdmType(String type) {
        switch (type) {
            case "string":
                return EdmPrimitiveTypeKind.String.getFullQualifiedName();
            case "integer":
                return EdmPrimitiveTypeKind.Int64.getFullQualifiedName();
            case "datetime":
                return EdmPrimitiveTypeKind.DateTimeOffset.getFullQualifiedName();
        }
        //fallback to string as default
        return EdmPrimitiveTypeKind.String.getFullQualifiedName();
    }

}
