import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import com.microsoft.aad.adal4j.AuthenticationContext;
import com.microsoft.aad.adal4j.AuthenticationResult;
import com.microsoft.aad.adal4j.ClientCredential;
import com.microsoft.azure.keyvault.KeyVaultClient;
import com.microsoft.azure.keyvault.authentication.KeyVaultCredentials;
import com.microsoft.azure.keyvault.models.KeyOperationResult;
import com.microsoft.azure.keyvault.webkey.JsonWebKeyEncryptionAlgorithm;
import com.microsoft.rest.credentials.ServiceClientCredentials;

import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class App {
    public static void main(String[] args) throws Exception {

        // Extract this out in .env
        String sharedAccessKey = args[0];
        String topic = args[1];
        String clientId = args[2];
        String clientSecret = args[3];
        String eventHub = args[4];

        Properties kafkaConfig = getKafkaConfig(sharedAccessKey, eventHub);

        processStream(kafkaConfig, topic, clientId, clientSecret);
    }

    private static void processStream(Properties kafkaConfig, String topic,
        String clientId, String clientSecret) throws Exception {

        KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<String, byte[]>(kafkaConfig);

        consumer.subscribe(Collections.singletonList(topic));

        System.out.println("Polling");

        String keyVaultKeyUri = null;
        byte[] secret = null;
        byte[] iv = null;

        try {
            while (true) {
                final ConsumerRecords<String, byte[]> consumerRecords = consumer.poll(1000);
                for (ConsumerRecord<String, byte[]> cr : consumerRecords) {
                    System.out.printf("Consumer Record:(%d, %d, %d, %d)\n", cr.key(), cr.value().length, cr.partition(),
                            cr.offset());

                    byte[] siv = cr.headers().lastHeader("symmetricKeyIV").value();
                    //siv = Arrays.copyOfRange(siv, 4, siv.length);
                    String symmetricKeyIV = new String(siv, StandardCharsets.UTF_8);
                    
                    byte[] sik = cr.headers().lastHeader("symmetricKey").value();
                    //sik = Arrays.copyOfRange(sik, 2, sik.length);
                    String symmetricKey = new String(sik, StandardCharsets.UTF_8);
                    
                    String keyVersion = new String(cr.headers().lastHeader("keyVersion").value(), StandardCharsets.UTF_8).substring(2);
                    String keyVaultKeyUriNew = new String(cr.headers().lastHeader("keyVaultKeyUri").value(), StandardCharsets.UTF_8).substring(2);
                    byte[] encryptedPayload = cr.value();

                    System.out.println(symmetricKeyIV);
                    System.out.println(symmetricKey);
                    System.out.println(keyVersion);
                    System.out.println(keyVaultKeyUriNew);

                    if(secret == null || ! keyVaultKeyUriNew.equals(keyVaultKeyUri)) {
                        // We've got a new key
                        secret = decryptKey(keyVaultKeyUriNew, symmetricKey, clientId, clientSecret);
                        iv = decryptKey(keyVaultKeyUriNew, symmetricKeyIV, clientId, clientSecret);
                        keyVaultKeyUri = keyVaultKeyUriNew;
                        System.out.println("IV Length " + iv.length);
                    }

                    SecretKeySpec secretKeySpec = new SecretKeySpec(secret, "AES");
                    IvParameterSpec ivParameterSpec = new IvParameterSpec(iv);
                    
                    Cipher cipherDecrypt = Cipher.getInstance("AES/CBC/PKCS5Padding");
                    cipherDecrypt.init(Cipher.DECRYPT_MODE, secretKeySpec, ivParameterSpec);
                    byte[] decrypted = cipherDecrypt.doFinal(encryptedPayload);

                    String decryptedText = new String(decrypted, StandardCharsets.UTF_8);

                    System.out.println(decryptedText);
                    //System.exit(0);
                }

                consumer.commitAsync();
            }
        } catch (CommitFailedException e) {
            System.out.println("CommitFailedException: " + e);
        } finally {
            consumer.close();
        }
    }

    public static byte[] decryptKey(String keyIdentifier, String textToDecrypt,
        String clientId, String clientSecret) throws Exception {

        ServiceClientCredentials credentials = createCredentials(clientId, clientSecret);
        
        KeyVaultClient keyVaultClient = new KeyVaultClient(credentials);

        byte[] byteText = Base64.getDecoder().decode(textToDecrypt);

        KeyOperationResult result = keyVaultClient.decrypt(keyIdentifier, JsonWebKeyEncryptionAlgorithm.RSA_OAEP, byteText);

        return result.result();
    }

    public static AuthenticationResult getAccessToken(String authorization, String resource, String clientId, String clientSecret) 
        throws Exception {
        
		if (clientId == null) {
			throw new Exception("Please inform arm.clientid in the environment settings.");
		}

		AuthenticationResult result = null;
		ExecutorService service = null;
		try {
			service = Executors.newFixedThreadPool(1);
			AuthenticationContext context = new AuthenticationContext(authorization, false, service);

			ClientCredential credentials = new ClientCredential(clientId, clientSecret);
			Future<AuthenticationResult> future = context.acquireToken(resource, credentials, null);

			if (future == null) {
				throw new Exception("Missing or ambiguous credentials.");
			}

			result = future.get();
		} finally {
			service.shutdown();
		}

		if (result == null) {
			throw new RuntimeException("authentication result was null");
		}
		return result;
    }

    
	private static ServiceClientCredentials createCredentials(final String clientId, final String clientSecret) throws Exception {
		return new KeyVaultCredentials() {

			@Override
			public String doAuthenticate(String authorization, String resource, String scope) {
				try {

                    String token = getAccessToken(authorization, resource, 
                        clientId, clientSecret).getAccessToken();

                    System.out.println("Working? " + token);

                    return token;

				} catch (Exception ex) {
					throw new RuntimeException(ex);
				}
			}
		};
	}

    private static Properties getKafkaConfig(String sharedAccessKey, String eventHub)
            throws IOException, FileNotFoundException {
        Properties kafkaConfig = new Properties();

        kafkaConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        kafkaConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());

        kafkaConfig.load(new FileReader("src/main/resources/consumer.config"));

        kafkaConfig.put("sasl.jaas.config",
                ((String) kafkaConfig.get("sasl.jaas.config")).replace("${ACCESS_KEY}", sharedAccessKey));

        kafkaConfig.put("bootstrap.servers",
                ((String) kafkaConfig.get("bootstrap.servers")).replace("${EVENT_HUB}", eventHub));

        kafkaConfig.put("sasl.jaas.config",
                ((String) kafkaConfig.get("sasl.jaas.config")).replace("${EVENT_HUB}", eventHub));
        return kafkaConfig;
    }
}
