package example;

import com.oracle.bmc.ConfigFileReader;
import com.oracle.bmc.Region;
import com.oracle.bmc.auth.BasicAuthenticationDetailsProvider;
import com.oracle.bmc.auth.ConfigFileAuthenticationDetailsProvider;
import com.oracle.bmc.auth.InstancePrincipalsAuthenticationDetailsProvider;
import com.oracle.bmc.hdfs.BmcProperties;
import com.oracle.bmc.http.ClientConfigurator;
import com.oracle.bmc.http.DefaultConfigurator;
import com.oracle.bmc.http.signing.internal.Constants;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.NoSuchElementException;
import javax.annotation.Priority;
import javax.ws.rs.Priorities;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.ClientRequestContext;
import javax.ws.rs.client.ClientRequestFilter;

/**
 * Customize the SDK underlying REST client to use the on-behalf-of token when running on
 * Data Flow.
 */
public class OboTokenClientConfigurator implements ClientConfigurator {

    // TODO: Set these values for your sepcific OCI environment
    public static final String LOCAL_PROFILE = "DEFAULT"; // TODO <your ~/.oci/config profile>
    private static final String CONFIG_FILE_PATH = ConfigFileReader.DEFAULT_FILE_PATH;
    private static final String CANONICAL_REGION_NAME = Region.US_ASHBURN_1.getRegionId();

    private final String delegationTokenPath;

    /**
     * Helper function for the Spark Driver to get the token path.
     */
    public static String getDelegationTokenPath() {
        SparkConf conf = new SparkConf();
        try {
            return conf.get("spark.hadoop.fs.oci.client.auth.delegationTokenPath");
        } catch (NoSuchElementException e) {
            return null;
        }
    }

    /**
     * Helper function to get the Hadoop configuration for the HDFS BmcFileSystem.
     */
    public static Configuration getConfiguration(Configuration config, String delegationTokenPath) {
        // https://objectstorage.us-phoenix-1.oraclecloud.com
        String domain = "oraclecloud.com";
        String overlayEndpoint = String
                .format("https://objectstorage.%s.%s", CANONICAL_REGION_NAME, domain);
        config.set(BmcProperties.HOST_NAME.getPropertyName(), overlayEndpoint);
        // Data Flow
        if (delegationTokenPath != null) {
            config.set("fs.oci.client.auth.delegationTokenPath", delegationTokenPath);
            config.set(BmcProperties.OBJECT_STORE_CLIENT_CLASS.getPropertyName(),
                    "oracle.dfcs.hdfs.DelegationObjectStorageClient");
        } else { // local
            try {
                ConfigFileAuthenticationDetailsProvider provider =
                        new ConfigFileAuthenticationDetailsProvider(CONFIG_FILE_PATH, LOCAL_PROFILE);
                config.set(BmcProperties.TENANT_ID.getPropertyName(), provider.getTenantId());
                config.set(BmcProperties.USER_ID.getPropertyName(), provider.getUserId());
                config.set(BmcProperties.FINGERPRINT.getPropertyName(), provider.getFingerprint());
                config.set(BmcProperties.PEM_FILE_PATH.getPropertyName(), "~/.oci/oci_api_key.pem");
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        }
        return config;
    }

    /**
     * Helper function to get an environment specific authentication provider.
     */
    public static BasicAuthenticationDetailsProvider getAuthProvider(String delegationTokenPath) {
        if (delegationTokenPath == null) { // local
            try {
                return new ConfigFileAuthenticationDetailsProvider(CONFIG_FILE_PATH, LOCAL_PROFILE);
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        }
        // Data Flow
        return InstancePrincipalsAuthenticationDetailsProvider.builder().build();
    }

    /**
     * Helper function to get an environment specific <tt>ClientConfigurator</tt>.
     */
    public static ClientConfigurator getConfigurator(String delegationTokenPath) {
        return (delegationTokenPath == null) ? new DefaultConfigurator() : // local
                new OboTokenClientConfigurator(delegationTokenPath); // Data Flow
    }

    /**
     * Helper function to get an environment specific working directory.
     */
    public static String getTempDirectory() {
        if (System.getenv("HOME").equals("/home/dataflow")) {
            return "/opt/spark/work-dir/";
        }
        return System.getProperty("java.io.tmpdir");
    }

    public OboTokenClientConfigurator(String delegationTokenPath) {
        this.delegationTokenPath = delegationTokenPath;
    }

    @Override
    public void customizeBuilder(ClientBuilder builder) {
    }

    @Override
    public void customizeClient(Client client) {
        client.register(new _OboTokenRequestFilter());
    }

    @Priority(_OboTokenRequestFilter.PRIORITY)
    class _OboTokenRequestFilter implements ClientRequestFilter {

        public static final int PRIORITY = Priorities.AUTHENTICATION - 1;

        @Override
        public void filter(final ClientRequestContext requestContext) throws IOException {
            String token = new String(Files.readAllBytes(Paths.get(delegationTokenPath)));
            requestContext.getHeaders().putSingle(Constants.OPC_OBO_TOKEN, token);
        }
    }
}