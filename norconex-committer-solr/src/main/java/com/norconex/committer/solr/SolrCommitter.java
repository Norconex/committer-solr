/* Copyright 2010-2019 Norconex Inc.
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
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

import org.apache.commons.configuration.HierarchicalConfiguration;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrInputDocument;

import com.norconex.committer.core.AbstractMappedCommitter;
import com.norconex.committer.core.CommitterException;
import com.norconex.committer.core.IAddOperation;
import com.norconex.committer.core.ICommitOperation;
import com.norconex.committer.core.IDeleteOperation;
import com.norconex.commons.lang.encrypt.EncryptionKey;
import com.norconex.commons.lang.encrypt.EncryptionUtil;
import com.norconex.commons.lang.map.Properties;
import com.norconex.commons.lang.time.DurationParser;
import com.norconex.commons.lang.xml.EnhancedXMLStreamWriter;

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
 * Since 2.4.0, BASIC authentication is supported for password-protected
 * Solr installations.
 * The <code>password</code> can optionally be
 * encrypted using {@link EncryptionUtil} (or command-line "encrypt.bat"
 * or "encrypt.sh").
 * In order for the password to be decrypted properly, you need
 * to specify the encryption key used to encrypt it. The key can be stored
 * in a few supported locations and a combination of
 * <code>passwordKey</code>
 * and <code>passwordKeySource</code> must be specified to properly
 * locate the key. The supported sources are:
 * </p>
 * <table border="1" summary="">
 *   <tr>
 *     <th><code>passwordKeySource</code></th>
 *     <th><code>passwordKey</code></th>
 *   </tr>
 *   <tr>
 *     <td><code>key</code></td>
 *     <td>The actual encryption key.</td>
 *   </tr>
 *   <tr>
 *     <td><code>file</code></td>
 *     <td>Path to a file containing the encryption key.</td>
 *   </tr>
 *   <tr>
 *     <td><code>environment</code></td>
 *     <td>Name of an environment variable containing the key.</td>
 *   </tr>
 *   <tr>
 *     <td><code>property</code></td>
 *     <td>Name of a JVM system property containing the key.</td>
 *   </tr>
 * </table>
 *
 * <p>
 * As of 2.2.1, XML configuration entries expecting millisecond durations
 * can be provided in human-readable format (English only), as per
 * {@link DurationParser} (e.g., "5 minutes and 30 seconds" or "5m30s").
 * </p>
 *
 * <h3>XML configuration usage:</h3>
 *
 * <pre>
 *  &lt;committer class="com.norconex.committer.solr.SolrCommitter"&gt;
 *      &lt;solrClientType&gt;
 *         (See class documentation for options. Default: HttpSolrClient.)
 *      &lt;/solrClientType&gt;
 *      &lt;solrURL&gt;(URL to Solr)&lt;/solrURL&gt;
 *      &lt;solrUpdateURLParams&gt;
 *         &lt;param name="(parameter name)"&gt;(parameter value)&lt;/param&gt;
 *         &lt;-- multiple param tags allowed --&gt;
 *      &lt;/solrUpdateURLParams&gt;
 *      &lt;solrCommitDisabled&gt;[false|true]&lt;/solrCommitDisabled&gt;
 *
 *      &lt;!-- Use the following if BASIC authentication is required. --&gt;
 *      &lt;username&gt;(Optional user name)&lt;/username&gt;
 *      &lt;password&gt;(Optional user password)&lt;/password&gt;
 *      &lt;!-- Use the following if password is encrypted. --&gt;
 *      &lt;passwordKey&gt;(the encryption key or a reference to it)&lt;/passwordKey&gt;
 *      &lt;passwordKeySource&gt;[key|file|environment|property]&lt;/passwordKeySource&gt;
 *
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


    private SolrClientType solrClientType;
    private String solrURL;
    private boolean solrCommitDisabled;
    private final Map<String, String> updateUrlParams = new HashMap<>();
    private String username;
    private String password;
    private EncryptionKey passwordKey;

    private SolrClient solrClient;

    /**
     * Constructor.
     */
    public SolrCommitter() {
        this(null);
    }
    /**
     * Constructor.
     * @param solrClient SolrClient to use
     */
    public SolrCommitter(SolrClient solrClient) {
        this.solrClient = solrClient;
        setTargetContentField(DEFAULT_SOLR_CONTENT_FIELD);
        setTargetReferenceField(DEFAULT_SOLR_ID_FIELD);
    }

    /**
     * Gets the Solr client type.
     * @return solr client type
     * @since 2.4.0
     */
    public SolrClientType getSolrClientType() {
        return solrClientType;
    }
    /**
     * Sets the Solr client type.
     * @param solrClientType solr client type
     * @since 2.4.0
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

    /**
     * Gets the username.
     * @return username the username
     * @since 2.4.0
     */
    public String getUsername() {
        return username;
    }
    /**
     * Sets the username.
     * @param username the username
     * @since 2.4.0
     */
    public void setUsername(String username) {
        this.username = username;
    }

    /**
     * Gets the password.
     * @return password the password
     * @since 2.4.0
     */
    public String getPassword() {
        return password;
    }
    /**
     * Sets the password.
     * @param password the password
     * @since 2.4.0
     */
    public void setPassword(String password) {
        this.password = password;
    }

    /**
     * Gets the password encryption key.
     * @return the password key or <code>null</code> if the password is not
     * encrypted.
     * @see EncryptionUtil
     * @since 2.4.0
     */
    public EncryptionKey getPasswordKey() {
        return passwordKey;
    }
    /**
     * Sets the password encryption key. Only required when
     * the password is encrypted.
     * @param passwordKey password key
     * @see EncryptionUtil
     * @since 2.4.0
     */
    public void setPasswordKey(EncryptionKey passwordKey) {
        this.passwordKey = passwordKey;
    }

    @Override
    protected void commitBatch(List<ICommitOperation> batch) {

        LOG.info("Sending " + batch.size()
                + " documents to Solr for update/deletion.");
        try {
            SolrClient solrClient = ensureSolrClient();

            // Add to request all operations in batch, and force a commit
            // whenever we do a "delete" after an "add" to eliminate the
            // risk of the delete being a no-op since added documents are
            // not visible until committed (thus nothing to delete).

            //TODO before a delete, check if the same reference was previously
            //added before forcing a commit if any additions occurred.
            boolean previousWasAddition = false;
            for (ICommitOperation op : batch) {
                UpdateRequest req = null;
                if (op instanceof IAddOperation) {
                    req = solrAddRequest((IAddOperation) op);
                    previousWasAddition = true;
                } else if (op instanceof IDeleteOperation) {
                    if (previousWasAddition && !isSolrCommitDisabled()) {
                        solrCommit();
                    }
                    req = solrDeleteRequest((IDeleteOperation) op);
                    previousWasAddition = false;
                } else {
                    throw new CommitterException("Unsupported operation:" + op);
                }

                setCredentials(req);

                for (Entry<String, String> entry : updateUrlParams.entrySet()) {
                    req.setParam(entry.getKey(), entry.getValue());
                }
                req.process(solrClient);
            }
            if (!isSolrCommitDisabled()) {
                solrCommit();
            }
        } catch (Exception e) {
          throw new CommitterException(
                  "Cannot index document batch to Solr.", e);
        }
        LOG.info("Done sending documents to Solr for update/deletion.");
    }

    private void setCredentials(UpdateRequest req) {
        if (StringUtils.isNotBlank(getUsername())) {
            req.setBasicAuthCredentials(getUsername(), EncryptionUtil.decrypt(
                    getPassword(), getPasswordKey()));
        }
    }

    private void solrCommit() throws IOException, SolrServerException {
        UpdateRequest req = new UpdateRequest();
        setCredentials(req);
        req.commit(solrClient, null);
    }

    protected UpdateRequest solrAddRequest(IAddOperation op) {
        UpdateRequest req = new UpdateRequest();
        req.add(buildSolrDocument(op.getMetadata()));
        return req;
    }
    protected UpdateRequest solrDeleteRequest(IDeleteOperation op) {
        UpdateRequest req = new UpdateRequest();
        req.deleteById(op.getReference());
        return req;
    }

    private synchronized SolrClient ensureSolrClient() {
        if (solrClient == null) {
            solrClient = ObjectUtils.defaultIfNull(solrClientType,
                    SolrClientType.HTTP).create(solrURL);
        }
        return solrClient;
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
        EnhancedXMLStreamWriter w = new EnhancedXMLStreamWriter(writer);

        if (solrClientType != null) {
            w.writeElementString("solrClientType", solrClientType.toString());
        }
        w.writeElementString("solrURL", solrURL);

        w.writeStartElement("solrUpdateURLParams");
        for (String name : updateUrlParams.keySet()) {
            w.writeStartElement("param");
            w.writeAttribute("name", name);
            w.writeCharacters(updateUrlParams.get(name));
            w.writeEndElement();
        }

        w.writeElementBoolean("solrCommitDisabled", isSolrCommitDisabled());
        w.writeElementString("username", getUsername());
        w.writeElementString("password", getPassword());
        // Encrypted password:
        EncryptionKey key = getPasswordKey();
        if (key != null) {
            w.writeElementString("passwordKey", key.getValue());
            if (key.getSource() != null) {
                w.writeElementString("passwordKeySource",
                        key.getSource().name().toLowerCase());
            }
        }

        w.writeEndElement();
    }

    @Override
    protected void loadFromXml(XMLConfiguration xml) {

        String xmlSolrClientType = xml.getString("solrClientType", null);
        if (StringUtils.isNotBlank(xmlSolrClientType)) {
            setSolrClientType(SolrClientType.of(xmlSolrClientType));
        }
        setSolrURL(xml.getString("solrURL", getSolrURL()));
        setSolrCommitDisabled(xml.getBoolean(
                "solrCommitDisabled", isSolrCommitDisabled()));
        List<HierarchicalConfiguration> uparams =
                xml.configurationsAt("solrUpdateURLParams.param");
        for (HierarchicalConfiguration param : uparams) {
            setUpdateUrlParam(param.getString("[@name]"), param.getString(""));
        }
        setUsername(xml.getString("username", getUsername()));
        setPassword(xml.getString("password", getPassword()));
        // encrypted password:
        String xmlKey = xml.getString("passwordKey", null);
        String xmlSource = xml.getString("passwordKeySource", null);
        if (StringUtils.isNotBlank(xmlKey)) {
            EncryptionKey.Source source = null;
            if (StringUtils.isNotBlank(xmlSource)) {
                source = EncryptionKey.Source.valueOf(xmlSource.toUpperCase());
            }
            setPasswordKey(new EncryptionKey(xmlKey, source));
        }
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder()
            .appendSuper(super.hashCode())
            .append(solrClientType)
            .append(solrURL)
            .append(updateUrlParams)
            .append(solrCommitDisabled)
            .append(username)
            .append(password)
            .append(passwordKey)
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
            .append(solrClientType, other.solrClientType)
            .append(solrURL, other.solrURL)
            .append(updateUrlParams, other.updateUrlParams)
            .append(solrCommitDisabled, other.solrCommitDisabled)
            .append(username, other.username)
            .append(password, other.password)
            .append(passwordKey, other.passwordKey)
            .isEquals();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .appendSuper(super.toString())
                .append("solrClientType", solrClientType)
                .append("solrURL", solrURL)
                .append("updateUrlParams", updateUrlParams)
                .append("solrCommitDisabled", solrCommitDisabled)
                .append("username", username)
                .append("password", "********")
                .append("passwordKey", "********")
                .toString();
    }
}
