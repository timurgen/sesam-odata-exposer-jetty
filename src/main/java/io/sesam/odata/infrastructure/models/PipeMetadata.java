package io.sesam.odata.infrastructure.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 *
 * @author 100tsa
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class PipeMetadata {

    public String type;
    @JsonProperty("source_property")
    public String sourceProperty;
    public String name;

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getSourceProperty() {
        return sourceProperty;
    }

    public void setSourceProperty(String sourceProperty) {
        this.sourceProperty = sourceProperty;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return "SesamMetadataObject{" + "type=" + type + ", sourceProperty=" + sourceProperty + ", name=" + name + '}';
    }

}
