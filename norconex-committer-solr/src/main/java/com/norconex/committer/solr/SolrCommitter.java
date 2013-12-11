/* Copyright 2010-2013 Norconex Inc.
 * 
 * This file is part of Norconex Committer Solr.
 * 
 * Norconex Committer Solr is free software: you can redistribute it and/or 
 * modify it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * Norconex Committer Solr is distributed in the hope that it will be useful, 
 * but WITHOUT ANY WARRANTY; without even the implied warranty of 
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with Norconex Committer Solr. If not, see 
 * <http://www.gnu.org/licenses/>.
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
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrInputDocument;

import com.norconex.committer.AbstractMappedCommitter;
import com.norconex.committer.CommitterException;
import com.norconex.committer.IAddOperation;
import com.norconex.committer.ICommitOperation;
import com.norconex.committer.IDeleteOperation;
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
 *      &lt;idSourceField keep="[false|true]"&gt;
 *         (Name of source field that will be mapped to the Solr "id" field
 *         or whatever "idTargetField" specified.
 *         Default is the document reference metadata field: 
 *         "document.reference".  Once re-mapped, the metadata source field is 
 *         deleted, unless "keep" is set to <code>true</code>.)
 *      &lt;/idSourceField&gt;
 *      &lt;idTargetField&gt;
 *         (Name of Solr target field where the store a document unique 
 *         identifier (idSourceField).  If not specified, default is "id".) 
 *      &lt;/idTargetField&gt;
 *      &lt;contentSourceField keep="[false|true]&gt;
 *         (If you wish to use a metadata field to act as the document 
 *         "content", you can specify that field here.  Default 
 *         does not take a metadata field but rather the document content.
 *         Once re-mapped, the metadata source field is deleted,
 *         unless "keep" is set to <code>true</code>.)
 *      &lt;/contentSourceField&gt;
 *      &lt;contentTargetField&gt;
 *         (Solr target field name for a document content/body.
 *          Default is: content)
 *      &lt;/contentTargetField&gt;
 *      &lt;queueDir&gt;(optional path where to queue files)&lt;/queueDir&gt;
 *      &lt;queueSize&gt;(queue size before committing)&lt;/queueSize&gt;
 *      &lt;commitBatchSize&gt;
 *          (max number of docs to send Solr at once)
 *      &lt;/commitBatchSize&gt;
 *  &lt;/committer&gt;
 * </pre>
 * 
 * @author Pascal Essiembre
 */
//TODO test if same files can be picked up more than once when multi-threading
public class SolrCommitter extends AbstractMappedCommitter {

    private static final long serialVersionUID = -842307672980791980L;
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
        setContentTargetField(DEFAULT_SOLR_CONTENT_FIELD);
        setIdTargetField(DEFAULT_SOLR_ID_FIELD);
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
     * Deprecated.
     * @return commit batch size
     * @deprecated use {@link #getCommitBatchSize()}
     */
    @Deprecated
    public int getSolrBatchSize() {
        LOG.warn("getSolrBatchSize() is deprecated. Use "
                + "getCommitBatchSize() instead.");
        return getCommitBatchSize();
    }
    /**
     * Deprecated
     * @param solrBatchSize commit batch size
     * @deprecated use {@link #setCommitBatchSize(int)}
     */
    public void setSolrBatchSize(int solrBatchSize) {
        LOG.warn("setSolrBatchSize(int) is deprecated. Use "
                + "setCommitBatchSize(int) instead.");
        setCommitBatchSize(solrBatchSize);
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
     * This method is now deprecated since updates and deletes are sent
     * on the same HTTP call.  Currently does the same thing as calling
     * <code>setUpdateUrlParam</code>.
     * @param name parameter name
     * @param value parameter value
     * @deprecated use {@link #setUpdateUrlParam(String, String)}
     */
    @Deprecated
    public void setDeleteUrlParam(String name, String value) {
        LOG.warn("setDeleteUrlParam(String, String) is deprecated. Use "
                + "setUpdateUrlParam(String, String) instead.");
        setUpdateUrlParam(name, value);
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
     * This method is now deprecated since updates and deletes are sent
     * on the same HTTP call.  Currently does the same thing as calling
     * <code>getUpdateUrlParam</code>.
     * @param name parameter name
     * @return parameter value
     * @deprecated use {@link #getUpdateUrlParam(String)}
     */
    @Deprecated
    public String getDeleteUrlParam(String name) {
        LOG.warn("getDeleteUrlParam(String) is deprecated. Use "
                + "getUpdateUrlParam(String) instead.");
        return getUpdateUrlParam(name);
    }
    /**
     * Gets the update URL parameter names.
     * @return parameter names
     */
    public Set<String> getUpdateUrlParamNames() {
        return updateUrlParams.keySet();
    }
    /**
     * This method is now deprecated since updates and deletes are sent
     * on the same HTTP call.  Currently does the same thing as calling
     * <code>getUpdateUrlParamNames</code>.
     * @return parameter names
     * @deprecated use {@link #getUpdateUrlParamNames()}
     */
    public Set<String> getDeleteUrlParamNames() {
        LOG.warn("getDeleteUrlParamNames() is deprecated. Use "
                + "getUpdateUrlParamNames() instead.");
        return getUpdateUrlParamNames();
    }

    @Override
    protected void commitBatch(List<ICommitOperation> batch) {

        LOG.info("Sending " + batch.size() 
                + " documents to Solr for update/deletion.");
        try {
            SolrServer server = solrServerFactory.createSolrServer(this);
            UpdateRequest request = new UpdateRequest();

            // Add to request any parameters provided
            for (String name : updateUrlParams.keySet()) {
                request.setParam(name, updateUrlParams.get(name));
            }

            // Add to request all opeations in batch
            for (ICommitOperation op : batch) {
                if (op instanceof IAddOperation) {
                    request.add(buildSolrDocument(
                            ((IAddOperation) op).getMetadata()));
                } else if (op instanceof IDeleteOperation) {
                    request.deleteById(((IDeleteOperation) op).getReference());
                } else {
                    throw new CommitterException("Unsupported operation:" + op);
                }
            }
            
            request.process(server);
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
    protected void loadFromXml(XMLConfiguration xml) {
        setSolrURL(xml.getString("solrURL", null));

        List<HierarchicalConfiguration> uparams = 
                xml.configurationsAt("solrUpdateURLParams.param");
        for (HierarchicalConfiguration param : uparams) {
            setUpdateUrlParam(param.getString("[@name]"), param.getString(""));
        }

        //--- Deprecated ---
        List<HierarchicalConfiguration> dparams = 
                xml.configurationsAt("solrDeleteURLParams.param");
        for (HierarchicalConfiguration param : dparams) {
            setDeleteUrlParam(param.getString("[@name]"), param.getString(""));
        }
        String batchSize = xml.getString("batchSize");
        if (StringUtils.isNotBlank(batchSize)) {
            LOG.warn("\"batchSize\" is deprecated. Use "
                    + "\"queueSize\" instead instead.");
            setQueueSize(Integer.parseInt(batchSize));
        }
        String solrBatchSize = xml.getString("solrBatchSize");
        if (StringUtils.isNotBlank(solrBatchSize)) {
            LOG.warn("\"solrBatchSize\" is deprecated. Use "
                    + "\"commitBatchSize\" instead instead.");
            setCommitBatchSize(Integer.parseInt(solrBatchSize));
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
     * Factory for creating and initalizing SolrServer instances.
     */
    public interface ISolrServerFactory extends Serializable {
        /**
         * Creats a new SolrServer.
         * @param solrCommitter this instance
         * @return a new SolrServer instance
         */
        SolrServer createSolrServer(SolrCommitter solrCommitter);
    }
    
    class DefaultSolrServerFactory implements ISolrServerFactory {
        private static final long serialVersionUID = 5820720860417411567L;
        private SolrServer server;
        @Override
        public SolrServer createSolrServer(SolrCommitter solrCommitter) {
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
