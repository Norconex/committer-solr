package com.norconex.committer.solr;


import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.norconex.committer.core3.CommitterContext;
import com.norconex.committer.core3.CommitterException;
import com.norconex.commons.lang.Sleeper;
import com.norconex.commons.lang.TimeIdGenerator;

/**
 * Base class for Solr tests requiring a running Solr server.  One Solr
 * instance is shared with all all tests in implementing class.
 * @author Pascal Essiembre
 */
@TestInstance(Lifecycle.PER_CLASS)
public abstract class AbstractSolrTest {

    private static final Logger LOG =
            LoggerFactory.getLogger(AbstractSolrTest.class);

    @TempDir
    static File tempDir;

    private File solrHome;
    private JettySolrRunner solrServer;
    private SolrClient solrClient;

    @BeforeAll
    private void beforeAll() throws Exception {

        solrHome = new File(tempDir, "solr-home");

        // Solr Server:
        LOG.info("Starting Solr test server...");
        LOG.info("  Solr home: {}", solrHome);

        if (solrServer != null && solrServer.isRunning()) {
            throw new IllegalStateException(
                    "Solr already running on local port "
                            + solrServer.getLocalPort());
        }
        System.setProperty("solr.log.dir",
                new File(solrHome, "solr-test.log").getAbsolutePath());


        FileUtils.moveDirectory(
                new File("./src/test/resources/solr-server"),
                solrHome);
                //new File(solrHome);
//        classpathFileToHome("core.properties", "test");
//        classpathFileToHome("schema.xml");
//        classpathFileToHome("solrconfig.xml");

        this.solrServer = new JettySolrRunner(
                solrHome.getAbsolutePath(), "/solr", 0);
        solrServer.start();

        int seconds = 0;
        for (; seconds < 30; seconds++) {
            if (solrServer.isRunning()) {
                break;
            }
            LOG.info("Waiting for Solr to start...");
            Sleeper.sleepSeconds(1);
        }
        if (seconds >= 30) {
            LOG.warn("Looks like Solr is not starting on port {}. "
                   + "Please investigate.", solrServer.getLocalPort());

        } else {
            LOG.info("Solr started on port {}", solrServer.getLocalPort());
        }

        // Solr Client:
        // solrServer.newClient() does not work for some reason, host is null
        solrClient = new HttpSolrClient.Builder(
               "http://localhost:" + getSolrPort() + "/solr/test").build();
    }
    @BeforeEach
    private void beforeEach() throws SolrServerException, IOException {
        solrClient.deleteByQuery("*:*");
        solrClient.commit();
    }
//    @AfterEach
//    private void afterEach() throws SolrServerException, IOException {
//    }
    @AfterAll
    private void afterAll() throws Exception {
        LOG.info("Stopping Solr.");
        solrClient.close();
        solrServer.stop();
        LOG.info("Solr stopped");
    }

    public int getSolrPort() {
        if (solrServer == null) {
            throw new IllegalStateException(
                    "Cannot get Solr port. Solr Server is not running.");
        }
        return solrServer.getLocalPort();
    }

    public File getSolrHome() {
        return solrHome;
    }

    public String getSolrTestURL() {
        return getSolrBaseURL() + "/test";
    }

    public String getSolrBaseURL() {
        if (solrServer == null) {
            throw new IllegalStateException(
                    "Cannot get Solr base URL. Solr Server is not running.");
        }
        return "http://localhost:" + getSolrPort()
                + solrServer.getBaseUrl().getPath();
    }

    public SolrClient getSolrClient() {
        return solrClient;
    }

    public SolrCommitter createSolrCommitter() throws CommitterException {
        CommitterContext ctx = CommitterContext.build()
                .setWorkDir(new File(getSolrHome(),
                        "" + TimeIdGenerator.next()).toPath())
                .create();
        SolrCommitter committer = new SolrCommitter();
        committer.setSolrURL(getSolrTestURL());
        committer.setUpdateUrlParam("commitWithin", "1");
        committer.init(ctx);
        return committer;
    }

//    public SolrClient newSolrClient() {
//        if (solrServer == null) {
//            throw new IllegalStateException(
//                    "Cannot create Solr client. Solr Server is not running.");
//        }
//        // solrServer.newClient() does not work for some reason, host is null
//        return new HttpSolrClient.Builder(
//               "http://localhost:" + getSolrPort() + "/solr/test").build();
//    }

    private void classpathFileToHome(String file)
            throws IOException {
        FileUtils.copyInputStreamToFile(
                getClass().getResourceAsStream("/" + file),
                new File(solrHome, file));
    }
}
