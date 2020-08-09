/* Copyright 2010-2020 Norconex Inc.
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

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.EqualsExclude;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.HashCodeExclude;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringExclude;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.norconex.committer.core3.CommitterException;
import com.norconex.committer.core3.DeleteRequest;
import com.norconex.committer.core3.ICommitterRequest;
import com.norconex.committer.core3.UpsertRequest;
import com.norconex.committer.core3.batch.AbstractBatchCommitter;
import com.norconex.commons.lang.encrypt.EncryptionUtil;
import com.norconex.commons.lang.io.IOUtil;
import com.norconex.commons.lang.map.Properties;
import com.norconex.commons.lang.security.Credentials;
import com.norconex.commons.lang.time.DurationParser;
import com.norconex.commons.lang.xml.XML;

/**
 * <p>
 * Commits documents to Apache Solr.
 * </p>
 *
 * <h3>Solr Client:</h3>
 * <p>
 * As of 2.4.0, it is possible to specify which type of
 * <a href="https://lucene.apache.org/solr/guide/8_1/using-solrj.html#types-of-solrclients">
 * Solr Client</a> to use.
 * The expected configuration value of "solrURL" is influenced
 * by the client type chosen.  Default client type is
 * <code>HttpSolrClient</code>. The clients are:
 * </p>
 * <dl>
 *   <dt>HttpSolrClient</dt>
 *     <dd>For direct access to a single Solr node. Ideal for
 *         local development. Needs a Solr URL. Default client.</dd>
 *   <dt>LBHttpSolrClient</dt>
 *     <dd>Simple load-balancing as an alternative to an external load balancer.
 *         Needs two or more Solr node URLs (comma-separated).</dd>
 *   <dt>ConcurrentUpdateSolrClient</dt>
 *     <dd>Optimized for mass upload on a single node.  Not best for queries.
 *         Needs a Solr URL.</dd>
 *   <dt>CloudSolrClient</dt>
 *     <dd>For use with a SolrCloud cluster. Needs a comma-separated list
 *         of Zookeeper hosts.</dd>
 *   <dt>Http2SolrClient</dt>
 *     <dd>Same as HttpSolrClient but for HTTP/2 support. Marked as
 *         experimental by Apache.</dd>
 *   <dt>LBHttp2SolrClient</dt>
 *     <dd>Same as LBHttpSolrClient but for HTTP/2 support. Marked as
 *         experimental by Apache.</dd>
 *   <dt>ConcurrentUpdateHttp2SolrClient</dt>
 *     <dd>Same as LBHttpSolrClient but for HTTP/2 support. Marked as
 *         experimental by Apache.</dd>
 * </dl>
 *
 * <h3>Authentication</h3>
 * <p>
 * BASIC authentication is supported for password-protected
 * Solr installations.
 * </p>
 *
 * {@nx.include com.norconex.commons.lang.security.Credentials#doc}
 *
 * {@nx.xml.usage
 * <committer class="com.norconex.committer.solr.SolrCommitter">
 *   <solrClientType>
 *     (See class documentation for options. Default: HttpSolrClient.)
 *   </solrClientType>
 *   <solrURL>(URL to Solr)</solrURL>
 *   <solrUpdateURLParams>
 *     <param name="(parameter name)">(parameter value)</param>
 *     <-- multiple param tags allowed -->
 *   </solrUpdateURLParams>
 *   <solrCommitDisabled>[false|true]</solrCommitDisabled>
 *
 *   <!-- Use the following if authentication is required. -->
 *   <credentials>
 *     {@nx.include com.norconex.commons.lang.security.Credentials@nx.xml.usage}
 *   </credentials>
 *
 *   <sourceReferenceField keep="[false|true]">
 *     (Optional name of field that contains the document reference, when
 *     the default document reference is not used.  The reference value
 *     will be mapped to Solr "id" field, or the "targetReferenceField"
 *     specified.
 *     Once re-mapped, this metadata source field is
 *     deleted, unless "keep" is set to true.)
 *   </sourceReferenceField>
 *   <targetReferenceField>
 *     (Name of Solr target field where the store a document unique
 *     identifier (idSourceField).  If not specified, default is "id".)
 *   </targetReferenceField>
 *   <sourceContentField keep="[false|true]">
 *     (If you wish to use a metadata field to act as the document
 *     "content", you can specify that field here.  Default
 *     does not take a metadata field but rather the document content.
 *     Once re-mapped, the metadata source field is deleted,
 *     unless "keep" is set to true.)
 *   </sourceContentField>
 *   <targetContentField>
 *     (Solr target field name for a document content/body.
 *     Default is: content)
 *   </targetContentField>
 *   <commitBatchSize>
 *     (Maximum number of docs to send Solr at once. Will issue a Solr
 *     commit unless "solrCommitDisabled" is true)
 *   </commitBatchSize>
 *   <queueDir>(optional path where to queue files)</queueDir>
 *   <queueSize>(max queue size before sending to Solr)</queueSize>
 *   <maxRetries>(max retries upon commit failures)</maxRetries>
 *   <maxRetryWait>(max delay in milliseconds between retries)</maxRetryWait>
 * </committer>
 * }
 *
 * <p>
 * As of 2.2.1, XML configuration entries expecting millisecond durations
 * can be provided in human-readable format (English only), as per
 * {@link DurationParser} (e.g., "5 minutes and 30 seconds" or "5m30s").
 * </p>
 *
 * @author Pascal Essiembre
 */
@SuppressWarnings("javadoc")
public class SolrCommitter extends AbstractBatchCommitter {

    private static final Logger LOG =
            LoggerFactory.getLogger(SolrCommitter.class);

    /** Default Solr ID field */
    public static final String DEFAULT_SOLR_ID_FIELD = "id";
    /** Default Solr content field */
    public static final String DEFAULT_SOLR_CONTENT_FIELD = "content";

    private SolrClientType solrClientType;
    private String solrURL;
    private boolean solrCommitDisabled;
    private final Map<String, String> updateUrlParams = new HashMap<>();
    private final Credentials credentials = new Credentials();

    @ToStringExclude
    @HashCodeExclude
    @EqualsExclude
    private SolrClient solrClient;

    /**
     * Constructor.
     */
    public SolrCommitter() {
        super();
    }

    /**
     * Gets the Solr client type.
     * @return solr client type
     */
    public SolrClientType getSolrClientType() {
        return solrClientType;
    }
    /**
     * Sets the Solr client type.
     * @param solrClientType solr client type
     */
    public void setSolrClientType(SolrClientType solrClientType) {
        this.solrClientType = solrClientType;
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
     * Sets whether to send an explicit commit request at the end of every
     * batch, or let the server auto-commit.
     * @param solrCommitDisabled <code>true</code> if sending Solr commit is
     *        disabled
     */
    public void setSolrCommitDisabled(boolean solrCommitDisabled) {
        this.solrCommitDisabled = solrCommitDisabled;
    }
    /**
     * Gets whether to send an explicit commit request at the end of every
     * batch, or let the server auto-commit.
     * @return <code>true</code> if sending Solr commit is disabled.
     */
    public boolean isSolrCommitDisabled() {
        return solrCommitDisabled;
    }

    /**
     * Gets Solr authentication credentials.
     * @return credentials
     */
    public Credentials getCredentials() {
        return credentials;
    }
    /**
     * Sets Solr authentication credentials.
     * @param credentials the credentials
     */
    public void setCredentials(Credentials credentials) {
        this.credentials.copyFrom(credentials);
    }

    @Override
    protected void initBatchCommitter() throws CommitterException {
        solrClient = ObjectUtils.defaultIfNull(solrClientType,
                SolrClientType.HTTP).create(solrURL);
    }
    @Override
    protected void commitBatch(Iterator<ICommitterRequest> it)
            throws CommitterException {

        // Add to request all operations in batch, and force a commit
        // whenever we do a "delete" after an "add" to eliminate the
        // risk of the delete being a no-op since added documents are
        // not visible until committed (thus nothing to delete).

        //TODO before a delete, check if the same reference was previously
        //added before forcing a commit if any additions occurred.

        int docCount = 0;
        try {
            final UpdateRequest solrBatchRequest = new UpdateRequest();
            boolean previousWasAddition = false;
            while (it.hasNext()) {
                ICommitterRequest r = it.next();
                if (r instanceof UpsertRequest) {
                    addSolrUpsertRequest(solrBatchRequest, (UpsertRequest) r);
                    previousWasAddition = true;
                } else if (r instanceof DeleteRequest) {
                    if (previousWasAddition) {
                        pushSolrRequest(solrBatchRequest);
                    }
                    addSolrDeleteRequest(solrBatchRequest, (DeleteRequest) r);
                    previousWasAddition = false;
                } else {
                    throw new CommitterException("Unsupported operation:" + r);
                }
                docCount++;
            }

            pushSolrRequest(solrBatchRequest);
            LOG.info("Sent {} committer operations to Solr.", docCount);

        } catch (CommitterException e) {
            throw e;
        } catch (Exception e) {
            throw new CommitterException(
                    "Cannot push document batch to Solr.", e);
        }
    }

    @Override
    protected void closeBatchCommitter() throws CommitterException {
        IOUtil.closeQuietly(solrClient);
        solrClient = null;
    }

    protected void pushSolrRequest(UpdateRequest solrBatchRequest)
            throws SolrServerException, IOException {

        if (credentials.isSet()) {
            solrBatchRequest.setBasicAuthCredentials(
                    credentials.getUsername(),
                    EncryptionUtil.decryptPassword(credentials));
        }
        for (Entry<String, String> entry : updateUrlParams.entrySet()) {
            solrBatchRequest.setParam(entry.getKey(), entry.getValue());
        }

        solrClient.request(solrBatchRequest);
        if (!isSolrCommitDisabled()) {
            solrClient.commit();
        }
        solrBatchRequest.clear();
    }

    protected void addSolrUpsertRequest(
            UpdateRequest solrBatchRequest, UpsertRequest committerRequest) {
        solrBatchRequest.add(buildSolrDocument(committerRequest.getMetadata()));
    }
    protected void addSolrDeleteRequest(
            UpdateRequest solrBatchRequest, DeleteRequest committerRequest) {
        solrBatchRequest.deleteById(committerRequest.getReference());
    }
    protected SolrInputDocument buildSolrDocument(Properties fields) {
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
    protected void loadBatchCommitterFromXML(XML xml) {
        setSolrClientType(xml.getEnum("solrClientType",
                SolrClientType.class, getSolrClientType()));
        setSolrURL(xml.getString("solrURL", getSolrURL()));
        setSolrCommitDisabled(xml.getBoolean(
                "solrCommitDisabled", isSolrCommitDisabled()));

        List<XML> paramsXML = xml.getXMLList("solrUpdateURLParams/param");
        if (!paramsXML.isEmpty()) {
            updateUrlParams.clear();
            paramsXML.forEach(p -> setUpdateUrlParam(
                    p.getString("@name"), p.getString(".")));
        }

        credentials.loadFromXML(xml.getXML("credentials"));
    }

    @Override
    protected void saveBatchCommitterToXML(XML xml) {
        xml.addElement("solrClientType", getSolrClientType());
        xml.addElement("solrURL", solrURL);

        if (!updateUrlParams.isEmpty()) {
            XML paramsXML = xml.addElement("solrUpdateURLParams");
            updateUrlParams.forEach((k, v) -> {
                paramsXML.addElement("param", v).setAttribute("name", k);
            });
        }

        xml.addElement("solrCommitDisabled", isSolrCommitDisabled());
        credentials.saveToXML(xml.addElement("credentials"));
    }

    @Override
    public boolean equals(final Object other) {
        return EqualsBuilder.reflectionEquals(this, other);
    }
    @Override
    public int hashCode() {
        return HashCodeBuilder.reflectionHashCode(this);
    }
    @Override
    public String toString() {
        return new ReflectionToStringBuilder(
                this, ToStringStyle.SHORT_PREFIX_STYLE).toString();
    }
}
