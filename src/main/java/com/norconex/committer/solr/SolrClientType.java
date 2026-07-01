/* Copyright 2019-2020 Norconex Inc.
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

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.ConcurrentUpdateHttp2SolrClient;
import org.apache.solr.client.solrj.impl.ConcurrentUpdateSolrClient;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.impl.LBHttp2SolrClient;
import org.apache.solr.client.solrj.impl.LBHttpSolrClient;

/**
 * Supported
 * <a href="https://lucene.apache.org/solr/guide/8_1/using-solrj.html#types-of-solrclients">
 * SolrClient</a> types.
 * @author Pascal Essiembre
 * @since 2.4.0
 */
public enum SolrClientType {

    /** Direct access to a single Solr node via HTTP. Default client type. */
    HTTP("HttpSolrClient", url -> new HttpSolrClient.Builder(url).build()),

    /** Simple load-balancing across multiple Solr nodes via HTTP. */
    LB_HTTP("LBHttpSolrClient",
            url -> new LBHttpSolrClient.Builder().withBaseSolrUrls(
                    url.split("[,\\s]+")).build()),

    /** Optimized for high-volume upserts to a single Solr node. */
    CONCURRENT_UPDATE("ConcurrentUpdateSolrClient",
            url -> new ConcurrentUpdateSolrClient.Builder(url).build()),

    /** Client for use with a SolrCloud cluster via ZooKeeper hosts. */
    CLOUD("CloudSolrClient", url -> {
        List<String> urls = Arrays.asList(url.split("[,\\s]+"));
        if (url.startsWith("http")) {
            return new CloudSolrClient.Builder(urls).build();
        }
        return new CloudSolrClient.Builder(urls, Optional.empty()).build();
    }),

    /** HTTP/2 variant of {@link #HTTP} (experimental). */
    HTTP2("Http2SolrClient", (url) -> new Http2SolrClient.Builder(url).build()),

    /** HTTP/2 variant of {@link #LB_HTTP} (experimental). */
    LB_HTTP2("LBHttp2SolrClient", url -> new LBHttp2SolrClient(
            new Http2SolrClient.Builder().build(), url.split("[,\\s]+"))),

    /** HTTP/2 variant of {@link #CONCURRENT_UPDATE} (experimental). */
    CONCURRENT_UPDATE_HTTP2("ConcurrentUpdateHttp2SolrClient", url ->
            new ConcurrentUpdateHttp2SolrClient.Builder(
                    url, new Http2SolrClient.Builder().build()).build())
    ;

    private final String type;
    private final Function<String, SolrClient> clientFactory;
    SolrClientType(String type, Function<String, SolrClient> f) {
        this.type = type;
        this.clientFactory = f;
    }

    @Override
    public String toString() {
        return type;
    }

    /**
     * Creates a {@link SolrClient} for the given Solr URL.
     * @param solrURL the Solr URL (or comma-separated URLs where applicable)
     * @return a new SolrClient instance
     */
    public SolrClient create(String solrURL) {
        Objects.requireNonNull(solrURL, "'solrURL' must not be null.");
        return clientFactory.apply(solrURL);
    }

    /**
     * Returns the {@code SolrClientType} matching the given type string
     * (case-insensitive), or {@code null} if none match.
     * @param type the client type name (e.g. "HttpSolrClient")
     * @return matching SolrClientType, or {@code null}
     */
    public static SolrClientType of(String type) {
        if (type == null) {
            return null;
        }
        for (SolrClientType t : SolrClientType.values()) {
            if (t.toString().equalsIgnoreCase(type)) {
                return t;
            }
        }
        return null;
    }
}
