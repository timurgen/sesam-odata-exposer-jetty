package io.sesam.odata.infrastructure.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 *
 * @author 100tsa
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class Dataset {

    @JsonProperty(value = "_id")
    public String id;
    
    public String camelCasedId;

    public Runtime runtime;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Runtime getRuntime() {
        return runtime;
    }

    public void setRuntime(Runtime runtime) {
        this.runtime = runtime;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Runtime {

        private String origin;

        public String getOrigin() {
            return origin;
        }

        public void setOrigin(String origin) {
            this.origin = origin;
        }

        @Override
        public String toString() {
            return "Runtime{" + "origin=" + origin + '}';
        }
        
        

    }

    @Override
    public String toString() {
        return "Dataset{" + "id=" + id + ", runtime=" + runtime + '}';
    }
    
    
}
