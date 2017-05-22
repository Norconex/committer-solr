/* Copyright 2010-2017 Norconex Inc.
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
import java.util.Map.Entry;
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
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;

import com.norconex.committer.core.AbstractMappedCommitter;
import com.norconex.committer.core.CommitterException;
import com.norconex.committer.core.IAddOperation;
import com.norconex.committer.core.ICommitOperation;
import com.norconex.committer.core.IDeleteOperation;
import com.norconex.commons.lang.map.Properties;
import com.norconex.commons.lang.time.DurationParser;

/**
 * Commits documents to Apache Solr.
 * <p>
 * XML configuration usage:
 * </p>
 * 
 * <p>
 * As of 2.2.1, XML configuration entries expecting millisecond durations
 * can be provided in human-readable format (English only), as per 
 * {@link DurationParser} (e.g., "5 minutes and 30 seconds" or "5m30s").
 * </p>
 * 
 * <pre>
 *  &lt;committer class="com.norconex.committer.solr.SolrCommitter"&gt;
 *      &lt;solrURL&gt;(URL to Solr)&lt;/solrURL&gt;
 *      &lt;solrUpdateURLParams&gt;
 *         &lt;param name="(parameter name)"&gt;(parameter value)&lt;/param&gt;
 *         &lt;-- multiple param tags allowed --&gt;
 *      &lt;/solrUpdateURLParams&gt;
 *      &lt;solrCommitDisabled&gt;[false|true]&lt;/solrCommitDisabled&gt;
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
 *         identifier (sourceReferenceField).  
 *         If not specified, default is "id".) 
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
 *         (Maximum number of docs to send Solr at once. Will issue a Solr 
 *          commit unless "solrCommitDisabled" is true)
 *      &lt;/commitBatchSize&gt;
 *      &lt;queueDir&gt;(optional path where to queue files)&lt;/queueDir&gt;
 *      &lt;queueSize&gt;(max queue size before sending to Solr)&lt;/queueSize&gt;
 *      &lt;maxRetries&gt;(max retries upon commit failures)&lt;/maxRetries&gt;
 *      &lt;maxRetryWait&gt;(max delay in milliseconds between retries)&lt;/maxRetryWait&gt;
 *  &lt;/committer&gt;
 * </pre>
 * 
 * @author Pascal Essiembre
 */
public class SolrCommitter extends AbstractMappedCommitter {

    private static final Logger LOG = LogManager.getLogger(SolrCommitter.class);

    /** Default Solr ID field */
    public static final String DEFAULT_SOLR_ID_FIELD = "id";
    /** Default Solr content field */
    public static final String DEFAULT_SOLR_CONTENT_FIELD = "content";

    private String solrURL;
    private boolean solrCommitDisabled;

    private final Map<String, String> updateUrlParams = new HashMap<>();
    
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
    /**
     * Set the variable to let the client know if it should commit or
     * let the server auto-commit.
     * @param commitDisabled <code>true</code> if commit is disabled
     * @since 2.1.0
     * @deprecated Since 2.2.0 use {@link #setSolrCommitDisabled(boolean)}
     */
    @Deprecated
    public void setCommitDisabled(boolean commitDisabled) {
        setSolrCommitDisabled(commitDisabled);
    }
    /**
     * Gets the commitDisabled variable to find out if the client should 
     * commit or let the server auto-commit.
     * @return <code>true</code> if commit is disabled.
     * @since 2.1.0
     * @deprecated Since 2.2.0 use {@link #isSolrCommitDisabled()}
     */
    @Deprecated
    public boolean isCommitDisabled() {
        return isSolrCommitDisabled();
    }

    /**
     * Sets whether to send an explicit commit request at the end of every
     * batch, or let the server auto-commit.
     * @param solrCommitDisabled <code>true</code> if sending Solr commit is 
     *        disabled
     * @since 2.2.0
     */
    public void setSolrCommitDisabled(boolean solrCommitDisabled) {
        this.solrCommitDisabled = solrCommitDisabled;
    }
    /**
     * Gets whether to send an explicit commit request at the end of every
     * batch, or let the server auto-commit.
     * @return <code>true</code> if sending Solr commit is disabled.
     * @since 2.2.0
     */
    public boolean isSolrCommitDisabled() {
        return solrCommitDisabled;
    }

    
    @Override
    protected void commitBatch(List<ICommitOperation> batch) {

        LOG.info("Sending " + batch.size() 
                + " documents to Solr for update/deletion.");
        try {
            SolrClient solrClient = solrServerFactory.createSolrServer(this);
            
            // Add to request all operations in batch, and force a commit
            // whenever we do a "delete" after an "add" to eliminate the
            // risk of the delete being a no-op since added documents are 
            // not visible until committed (thus nothing to delete).
            
            //TODO before a delete, check if the same reference was previously
            //added before forcing a commit if any additions occurred.
            boolean previousWasAddition = false;
            for (ICommitOperation op : batch) {
                if (op instanceof IAddOperation) {
                    solrClient.add(buildSolrDocument(
                            ((IAddOperation) op).getMetadata()));
                    previousWasAddition = true;
                } else if (op instanceof IDeleteOperation) {
                    if (previousWasAddition) {
                        if (!isSolrCommitDisabled()) {
                            solrClient.commit();
                        }
                    }
                    solrClient.deleteById(
                            ((IDeleteOperation) op).getReference());
                    previousWasAddition = false;
                } else {
                    throw new CommitterException("Unsupported operation:" + op);
                }
            }
            if (!isSolrCommitDisabled()) {
                solrClient.commit();
            }
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

        writer.writeStartElement("solrCommitDisabled");
        writer.writeCharacters(Boolean.toString(isSolrCommitDisabled()));
        writer.writeEndElement();
        
        writer.writeEndElement();
    }

    @Override
    @SuppressWarnings("unchecked")
    protected void loadFromXml(XMLConfiguration xml) {
        setSolrURL(xml.getString("solrURL", getSolrURL()));
        
        String commitDisabled = xml.getString("commitDisabled", null);
        if (StringUtils.isNotBlank(commitDisabled)) {
            LOG.warn("\"commitDisabled\" was renamed to "
                    + "\"solrCommitDisabled\".");
            setSolrCommitDisabled(Boolean.valueOf(commitDisabled));
        } else {
            setSolrCommitDisabled(xml.getBoolean(
                    "solrCommitDisabled", isSolrCommitDisabled()));
        }
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
            .append(solrCommitDisabled)
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
            .append(solrCommitDisabled, other.solrCommitDisabled)
            .isEquals();
    }
    
    @Override
    public String toString() {
        return "SolrCommitter [solrURL=" + solrURL + ", updateUrlParams="
                + updateUrlParams + ", solrServerFactory=" + solrServerFactory
                + ", solrCommitDisabled=" + solrCommitDisabled 
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
        private HttpSolrClient server;
        @Override
        public synchronized SolrClient createSolrServer(
                SolrCommitter solrCommitter) {
            if (server == null) {
                if (StringUtils.isBlank(solrCommitter.getSolrURL())) {
                    throw new CommitterException("Solr URL is undefined.");
                }
                
                server = new CommitterSolrClient(solrCommitter.getSolrURL());
                for (Entry<String, String> entry : 
                        solrCommitter.updateUrlParams.entrySet()) {
                    server.getInvariantParams().set(
                            entry.getKey(), entry.getValue());
                }
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

    // This class is to fix the HttpSolrClient having a null invariantParams
    // variable with no way to set it.
    static class CommitterSolrClient extends HttpSolrClient {
        private static final long serialVersionUID = 4152566496035344194L;
        @SuppressWarnings("deprecation")
        public CommitterSolrClient(String baseURL) {
            // We have to use the straight constructor version even if 
            // deprecated in order to initialise the invariantParams.
            super(baseURL);
            invariantParams = new ModifiableSolrParams();
        }
    }
}
