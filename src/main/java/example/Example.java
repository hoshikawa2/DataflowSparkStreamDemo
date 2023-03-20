package example;

import com.oracle.bmc.Region;
import com.oracle.bmc.auth.AbstractAuthenticationDetailsProvider;
import com.oracle.bmc.auth.BasicAuthenticationDetailsProvider;
import com.oracle.bmc.http.ClientConfigurator;
import com.oracle.bmc.secrets.Secrets;
import com.oracle.bmc.secrets.SecretsClient;
import com.oracle.bmc.secrets.model.Base64SecretBundleContentDetails;
import com.oracle.bmc.secrets.requests.GetSecretBundleRequest;
import com.oracle.bmc.secrets.responses.GetSecretBundleResponse;
import org.apache.commons.codec.binary.Base64;

import java.net.URI;
import java.nio.charset.StandardCharsets;

import com.oracle.bmc.auth.ResourcePrincipalAuthenticationDetailsProvider;

import oracle.jdbc.driver.OracleConnection;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.*;
import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Map;


import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.concurrent.TimeoutException;
import static org.apache.spark.sql.functions.*;

public class Example {

	public static void main(String[] args) throws Exception {

		Logger log = LogManager.getLogger(Example.class);
		log.info("Started StructuredKafkaWordCount");

		Thread.setDefaultUncaughtExceptionHandler((thread, e) -> {
			log.error("Exception uncaught: ", e);
		});

		//Uncomment following line to enable debug log level.
		//Logger.getRootLogger().setLevel(Level.DEBUG);

		String bootstrapServers = "cell-1.streaming.us-ashburn-1.oci.oraclecloud.com:9092";
		String topics = "kafka_like";
		String streamPoolId = "ocid1.streampool.oc1.iad.a...............................5a";
		String kafkaUsername = "<tenancyName>/oracleidentitycloudservice/<userNameEmail>/" + streamPoolId;
		String kafkaPassword = "<userNameEmailToken>";
		String jaasTemplate = "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";";
		String jaasCfg = String.format(jaasTemplate, kafkaUsername, kafkaPassword);

		String OBJECT_STORAGE_NAMESPACE = "<tenancyNamespace>";
		String checkpointLocation = "streamOutput/";
		String type = "csv";
		String outputLocation = "oci://data@" + OBJECT_STORAGE_NAMESPACE + "/";

		String OCI_URI_WALLET = "oci://Wallet@" + OBJECT_STORAGE_NAMESPACE;
		String DATABASE_NAME = "logs";
		String PASSWORD_SECRET_OCID = "ocid1.vaultsecret.oc1.iad.a...................................a";
		String WALLET_PATH = OCI_URI_WALLET + "/Wallet_" + DATABASE_NAME + ".zip";
		String TNS_NAME = DATABASE_NAME + "_high";
		String USER="ADMIN";
		String passwordOcid = PASSWORD_SECRET_OCID; // TODO <the vault secret OCID>
		String user = "ADMIN"; //DB user name
		String tnsName = TNS_NAME; // this can be found inside of the wallet.zip (unpack it), then open tnsnames.ora
		String source = null;

		Map<String, String> options = new HashMap<String, String>();

		switch (type) {
			case "console":
				System.err.println("Using console output sink");
				break;

			case "csv":
				System.err.println("Using csv output sink, output location = " + outputLocation);
				break;

			default:
				printUsage();
		}

		SparkSession spark;
		BasicAuthenticationDetailsProvider provider = null;

		// ---------------------------------------------------------------------
		// 1 - SparkSession
		System.out.println("---------------------------------------------------------------------------------");
		System.out.println("1 - SparkSession");
		JavaSparkContext jsc = null;

		if (DataFlowSparkSession.isRunningInDataFlow()) {
			spark = SparkSession.builder()
					.appName("StructuredKafkaWordCount")
					.config("spark.sql.streaming.minBatchesToRetain", "10")
					.config("spark.sql.shuffle.partitions", "1")
					.config("spark.sql.streaming.stateStore.maintenanceInterval", "300")
					.getOrCreate();
			provider = ResourcePrincipalAuthenticationDetailsProvider.builder().build();
			options.put("walletUri", WALLET_PATH);
			options.put("connectionId", TNS_NAME);
			options.put("user", USER);
			source = "oracle";
		} else {
			spark = SparkSession.builder()
					.appName("StructuredKafkaWordCount")
					.master("local[*]")
					.config("spark.sql.streaming.minBatchesToRetain", "10")
					.config("spark.sql.shuffle.partitions", "1")
					.config("spark.sql.streaming.stateStore.maintenanceInterval", "300")
					.getOrCreate();
			provider = OboTokenClientConfigurator.getAuthProvider(null);

			// Download the wallet from object storage and distribute it.
			jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());
			String delegationTokenPath = OboTokenClientConfigurator.getDelegationTokenPath();
			String tmpPath = DataFlowDeployWallet.deployWallet(new URI(OCI_URI_WALLET), spark.sparkContext(),
					OboTokenClientConfigurator.getConfiguration(jsc.hadoopConfiguration(), delegationTokenPath), WALLET_PATH);

			String jdbcUrl = MessageFormat.format("jdbc:oracle:thin:@{0}?TNS_ADMIN={1}", tnsName, tmpPath);
			System.out.println("JDBC URL " + jdbcUrl);

			options.put("driver", "oracle.jdbc.driver.OracleDriver");
			options.put("url", jdbcUrl);
			options.put(OracleConnection.CONNECTION_PROPERTY_USER_NAME, user);
			options.put(OracleConnection.CONNECTION_PROPERTY_TNS_ADMIN, tmpPath);
			source = "jdbc";
		}
		System.out.println("source=" + source);
		// ---------------------------------------------------------------------

		// ---------------------------------------------------------------------
		// 2 - Secret Vault
		System.out.println("---------------------------------------------------------------------------------");
		System.out.println("2 - Secret Vault");
		SecretsClient secretsClient;

		secretsClient = new SecretsClient(provider);
		secretsClient.setRegion(Region.US_ASHBURN_1);

		String password = new String(getSecret(passwordOcid, secretsClient));
		// ---------------------------------------------------------------------

		// 3 - Query a table from ADW: SELECT
		System.out.println("---------------------------------------------------------------------------------");
		System.out.println("3 - Query a table from ADW: SELECT");
		options.put("password", password);
		options.put("query", "select * from gdppercapta");

		Dataset<Row> oracleDF2 = spark.read().format(source).options(options).load();
		oracleDF2.show();
		// ---------------------------------------------------------------------

		// 4 - Kafka
		System.out.println("---------------------------------------------------------------------------------");
		System.out.println("4 - Kafka");
		// Create DataFrame representing the stream of input lines from Kafka
		Dataset<Row> lines = spark
				.readStream()
				.format("kafka")
				.option("kafka.bootstrap.servers", bootstrapServers)
				.option("subscribe", topics)
				.option("kafka.security.protocol", "SASL_SSL")
				.option("kafka.sasl.mechanism", "PLAIN")
				.option("kafka.sasl.jaas.config", jaasCfg)
				.option("kafka.max.partition.fetch.bytes", 1024 * 1024) // limit request size to 1MB per partition
				.option("startingOffsets", "latest")
				.load();

		Dataset<Row> filteredDataset = lines.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
				.filter("value IS NOT NULL");

		StructType schema = new StructType()
				.add("Organization Id", DataTypes.StringType)
				.add("Name", DataTypes.StringType)
				.add("Country", DataTypes.StringType);

		Dataset<Row> formattedDataset = filteredDataset
				.select(from_json(col("value"), schema).as("json"))
				.select(col("json.Organization Id"), col("json.Name"), col("json.Country"));

		System.out.println("From kafka");
		StreamingQuery queryFormatted = formattedDataset.writeStream().format("console").start();
		// ---------------------------------------------------------------------

		// ---------------------------------------------------------------------
		// 5 - Merge data
		System.out.println("---------------------------------------------------------------------------------");
		System.out.println("5 - Merge data");
		formattedDataset.createOrReplaceTempView("organizations");
		oracleDF2.createOrReplaceTempView("gdppercapta");

		Dataset<Row> finalDataset = spark.sql("SELECT a.`organization id` as organization, a.name, a.country, b.area FROM organizations a, gdppercapta b where a.country = b.country");
		StreamingQuery queryFinal = finalDataset.writeStream().format("console").start();
		// ---------------------------------------------------------------------

		// Start streaming query
		StreamingQuery query = null;
		switch (type) {
			case "console":
				query = outputToConsole(finalDataset, checkpointLocation);
				break;
			case "csv":
				query = outputToCsv(finalDataset, checkpointLocation, outputLocation);
				break;
			default:
				System.err.println("Unknown type " + type);
				System.exit(1);
		}

		query.awaitTermination();
		queryFormatted.awaitTermination();
		queryFinal.awaitTermination();
		if (jsc != null) { jsc.close(); }

	}

	private static void printUsage() {
		System.err.println("Usage: StructuredKafkaWordCount <bootstrap-servers> " +
				"<subscribe-topics> <checkpoint-location> <type> ...");
		System.err.println("<type>: console");
		System.err.println("<type>: csv <output-location>");
		System.err.println("<type>: adw <wallet-path> <wallet-password> <tns-name>");
		System.exit(1);
	}

	private static StreamingQuery outputToConsole(Dataset<Row> wordCounts, String checkpointLocation)
			throws TimeoutException {
		return wordCounts
				.writeStream()
				.format("console")
				.outputMode("complete")
				.option("checkpointLocation", checkpointLocation)
				.start();
	}

	private static StreamingQuery outputToCsv(Dataset<Row> wordCounts, String checkpointLocation,
											  String outputLocation) throws TimeoutException {
		return wordCounts
				.writeStream()
				.format("csv")
				.outputMode("append")
				.option("checkpointLocation", checkpointLocation)
				.trigger(Trigger.ProcessingTime("1 minutes"))
				.option("path", outputLocation)
				.start();
	}

	private static StreamingQuery outputToADW(Dataset<Row> wordCounts, String walletUri,
											  String password, String user) throws TimeoutException {
		return wordCounts
				.writeStream()
				.format("oracle")
				.option("walletUri","oci://<bucket>@<namespace>/Wallet_DATABASE.zip")
				.option("connectionId","database_medium")
				.option("dbtable", "schema.tablename")
				.start();
	}

	public static byte[] getSecret(String secretOcid, SecretsClient secretsClient) {
		GetSecretBundleRequest getSecretBundleRequest = GetSecretBundleRequest
				.builder()
				.secretId(secretOcid)
				.stage(GetSecretBundleRequest.Stage.Current)
				.build();
		GetSecretBundleResponse getSecretBundleResponse = secretsClient
				.getSecretBundle(getSecretBundleRequest);
		Base64SecretBundleContentDetails base64SecretBundleContentDetails =
				(Base64SecretBundleContentDetails) getSecretBundleResponse.
						getSecretBundle().getSecretBundleContent();
		byte[] secretValueDecoded = Base64.decodeBase64(base64SecretBundleContentDetails.getContent());
		return secretValueDecoded;
	}
}