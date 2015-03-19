/* Copyright 2010-2015 Norconex Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.norconex.committer.solr;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

import org.apache.commons.configuration.HierarchicalConfiguration;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrInputDocument;

import com.norconex.committer.core.AbstractMappedCommitter;
import com.norconex.committer.core.CommitterException;
import com.norconex.committer.core.IAddOperation;
import com.norconex.committer.core.ICommitOperation;
import com.norconex.committer.core.IDeleteOperation;
import com.norconex.commons.lang.map.Properties;

/**
 * Commits documents to Apache Solr.
 * <p>
 * XML configuration usage:
 * </p>
 * 
 * <pre>
 *  &lt;committer class="com.norconex.committer.solr.SolrCommitter"&gt;
 *      &lt;solrURL&gt;(URL to Solr)&lt;/solrURL&gt;
 *      &lt;solrUpdateURLParams&gt;
 *         &lt;param name="(parameter name)"&gt;(parameter value)&lt;/param&gt;
 *         &lt;-- multiple param tags allowed --&gt;
 *      &lt;/solrUpdateURLParams&gt;
 *      &lt;sourceReferenceField keep="[false|true]"&gt;
 *         (Optional name of field that contains the document reference, when 
 *         the default document reference is not used.  The reference value
 *         will be mapped to Solr "id" field, or the "targetReferenceField" 
 *         specified.
 *         Once re-mapped, this metadata source field is 
 *         deleted, unless "keep" is set to <code>true</code>.)
 *      &lt;/sourceReferenceField&gt;
 *      &lt;targetReferenceField&gt;
 *         (Name of Solr target field where the store a document unique 
 *         identifier (idSourceField).  If not specified, default is "id".) 
 *      &lt;/targetReferenceField&gt;
 *      &lt;sourceContentField keep="[false|true]"&gt;
 *         (If you wish to use a metadata field to act as the document 
 *         "content", you can specify that field here.  Default 
 *         does not take a metadata field but rather the document content.
 *         Once re-mapped, the metadata source field is deleted,
 *         unless "keep" is set to <code>true</code>.)
 *      &lt;/sourceContentField&gt;
 *      &lt;targetContentField&gt;
 *         (Solr target field name for a document content/body.
 *          Default is: content)
 *      &lt;/targetContentField&gt;
 *      &lt;commitBatchSize&gt;
 *          (max number of docs to send Solr at once)
 *      &lt;/commitBatchSize&gt;
 *      &lt;queueDir&gt;(optional path where to queue files)&lt;/queueDir&gt;
 *      &lt;queueSize&gt;(max queue size before committing)&lt;/queueSize&gt;
 *      &lt;maxRetries&gt;(max retries upon commit failures)&lt;/maxRetries&gt;
 *      &lt;maxRetryWait&gt;(max delay between retries)&lt;/maxRetryWait&gt;
 *  &lt;/committer&gt;
 * </pre>
 * 
 * @author Pascal Essiembre
 */
//TODO test if same files can be picked up more than once when multi-threading
public class SolrCommitter extends AbstractMappedCommitter {

    private static final Logger LOG = LogManager.getLogger(SolrCommitter.class);

    /** Default Solr ID field */
    public static final String DEFAULT_SOLR_ID_FIELD = "id";
    /** Default Solr content field */
    public static final String DEFAULT_SOLR_CONTENT_FIELD = "content";

    private String solrURL;

    private final Map<String, String> updateUrlParams = 
            new HashMap<String, String>();
    
    private final ISolrServerFactory solrServerFactory;

    /**
     * Constructor.
     */
    public SolrCommitter() {
        this(null);
    }
    /**
     * Constructor.
     * @param solrServerFactory Solr server factory
     */
    public SolrCommitter(ISolrServerFactory solrServerFactory) {
        if (solrServerFactory == null) {
            this.solrServerFactory = new DefaultSolrServerFactory();
        } else {
            this.solrServerFactory = solrServerFactory;
        }
        setTargetContentField(DEFAULT_SOLR_CONTENT_FIELD);
        setTargetReferenceField(DEFAULT_SOLR_ID_FIELD);
    }

    /**
     * Gets the Solr URL.
     * @return Solr URL
     */
    public String getSolrURL() {
        return solrURL;
    }
    /**
     * Sets the Solr URL.
     * @param solrURL solrURL
     */
    public void setSolrURL(String solrURL) {
        this.solrURL = solrURL;
    }

    /**
     * Sets URL parameters to be added on Solr HTTP calls.
     * @param name parameter name
     * @param value parameter value
     */
    public void setUpdateUrlParam(String name, String value) {
        updateUrlParams.put(name, value);
    }
    /**
     * Gets a URL parameter value by its parameter name.
     * @param name parameter name
     * @return parameter value
     */
    public String getUpdateUrlParam(String name) {
        return updateUrlParams.get(name);
    }
    /**
     * Gets the update URL parameter names.
     * @return parameter names
     */
    public Set<String> getUpdateUrlParamNames() {
        return updateUrlParams.keySet();
    }

    @Override
    protected void commitBatch(List<ICommitOperation> batch) {

        LOG.info("Sending " + batch.size() 
                + " documents to Solr for update/deletion.");
        try {
            SolrClient server = solrServerFactory.createSolrServer(this);
            
            UpdateRequest request = new UpdateRequest();
            // Add to request any parameters provided
            for (String name : updateUrlParams.keySet()) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("name = " + "updateUrlParams = " 
                            + updateUrlParams.get(name));
                }
                request.setParam(name, updateUrlParams.get(name));               
            }

            // Add to request all operations in batch, and force a commit
            // whenever we do a "delete" after an "add" to eliminate the
            // risk of the delete being a no-op since added documents are 
            // not visible until committed (thus nothing to delete).
            
            //TODO before a delete, check if the same reference was previously
            //added before forcing a commit if any additions occurred.
            //TODO figure out why the committing fix does not always work
            boolean previousWasAddition = false;
            for (ICommitOperation op : batch) {
                if (op instanceof IAddOperation) {
                    server.add(buildSolrDocument(
                            ((IAddOperation) op).getMetadata()));
                    previousWasAddition = true;
                } else if (op instanceof IDeleteOperation) {
                    if (previousWasAddition) {
                        server.commit();
                    }
                    server.deleteById(((IDeleteOperation) op).getReference());
                    previousWasAddition = false;
                } else {
                    throw new CommitterException("Unsupported operation:" + op);
                }
            }
            server.commit();
        } catch (Exception e) {
          throw new CommitterException(
                  "Cannot index document batch to Solr.", e);
        }
        LOG.info("Done sending documents to Solr for update/deletion.");    
    }
    
    private SolrInputDocument buildSolrDocument(Properties fields) {
        SolrInputDocument doc = new SolrInputDocument();
        for (String key : fields.keySet()) {
            List<String> values = fields.getStrings(key);
            for (String value : values) {
                doc.addField(key, value);
            }
        }
        return doc;
    }

    @Override
    protected void saveToXML(XMLStreamWriter writer) throws XMLStreamException {
        writer.writeStartElement("solrURL");
        writer.writeCharacters(solrURL);
        writer.writeEndElement();

        writer.writeStartElement("solrUpdateURLParams");
        for (String name : updateUrlParams.keySet()) {
            writer.writeStartElement("param");
            writer.writeAttribute("name", name);
            writer.writeCharacters(updateUrlParams.get(name));
            writer.writeEndElement();
        }
        writer.writeEndElement();
    }

    @Override
    @SuppressWarnings("unchecked")
    protected void loadFromXml(XMLConfiguration xml) {
        setSolrURL(xml.getString("solrURL", null));

        List<HierarchicalConfiguration> uparams = 
                xml.configurationsAt("solrUpdateURLParams.param");
        for (HierarchicalConfiguration param : uparams) {
            setUpdateUrlParam(param.getString("[@name]"), param.getString(""));
        }
    }

    
    @Override
    public int hashCode() {
        return new HashCodeBuilder()
            .appendSuper(super.hashCode())
            .append(solrServerFactory)
            .append(solrURL)
            .append(updateUrlParams)
            .toHashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof SolrCommitter)) {
            return false;
        }
        SolrCommitter other = (SolrCommitter) obj;
        return new EqualsBuilder()
            .appendSuper(super.equals(obj))
            .append(solrServerFactory, other.solrServerFactory)
            .append(solrURL, other.solrURL)
            .append(updateUrlParams, other.updateUrlParams)
            .isEquals();
    }
    
    @Override
    public String toString() {
        return "SolrCommitter [solrURL=" + solrURL + ", updateUrlParams="
                + updateUrlParams + ", solrServerFactory=" + solrServerFactory
                + ", " + super.toString() + "]";
    }

    //TODO make it a top-level interface?  Make it XMLConfigurable?
    /**
     * Factory for creating and initializing SolrServer instances.
     */
    public interface ISolrServerFactory extends Serializable {
        /**
         * Creates a new SolrServer.
         * @param solrCommitter this instance
         * @return a new SolrServer instance
         */
        SolrClient createSolrServer(SolrCommitter solrCommitter);
    }
    
    static class DefaultSolrServerFactory implements ISolrServerFactory {
        private static final long serialVersionUID = 5820720860417411567L;
        private SolrClient server;
        @Override
        public SolrClient createSolrServer(SolrCommitter solrCommitter) {
            if (server == null) {
                if (StringUtils.isBlank(solrCommitter.getSolrURL())) {
                    throw new CommitterException("Solr URL is undefined.");
                }
               server = new HttpSolrServer(solrCommitter.getSolrURL());
            }
            return server;
        }
        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result
                    + ((server == null) ? 0 : server.hashCode());
            return result;
        }
        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            DefaultSolrServerFactory other = (DefaultSolrServerFactory) obj;
            if (server == null) {
                if (other.server != null) {
                    return false;
                }
            } else if (!server.equals(other.server)) {
                return false;
            }
            return true;
        }
        @Override
        public String toString() {
            return "DefaultSolrServerFactory [server=" + server + "]";
        }
    }

}
