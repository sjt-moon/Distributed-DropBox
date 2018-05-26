package surfstore;

import java.io.File;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import surfstore.SurfStoreBasic.Empty;

import java.io.UnsupportedEncodingException;
import com.google.protobuf.ByteString;

import surfstore.SurfStoreBasic.*;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;

class HashUtils {
    public static String sha256(String str) {
        MessageDigest digest = null;
        try {
            digest = MessageDigest.getInstance("SHA-256");
        }
        catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
            System.exit(2);
        }
        byte[] hash = digest.digest(str.getBytes(StandardCharsets.UTF_8));
        String encoded = Base64.getEncoder().encodeToString(hash);
        return encoded;
    }
}

public final class Client {
    private static final Logger logger = Logger.getLogger(Client.class.getName());

    private final ManagedChannel metadataChannel;
    private final MetadataStoreGrpc.MetadataStoreBlockingStub metadataStub;

    private final ManagedChannel blockChannel;
    private final BlockStoreGrpc.BlockStoreBlockingStub blockStub;

    private final ConfigReader config;

    public Client(ConfigReader config) {
        this.metadataChannel = ManagedChannelBuilder.forAddress("127.0.0.1", config.getMetadataPort(1))
                .usePlaintext(true).build();
        this.metadataStub = MetadataStoreGrpc.newBlockingStub(metadataChannel);

        this.blockChannel = ManagedChannelBuilder.forAddress("127.0.0.1", config.getBlockPort())
                .usePlaintext(true).build();
        this.blockStub = BlockStoreGrpc.newBlockingStub(blockChannel);

        this.config = config;
    }

    public void shutdown() throws InterruptedException {
        metadataChannel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        blockChannel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

  private static Block stringToBlock(String s) {
    Block.Builder builder = Block.newBuilder();
    try {
      builder.setData(ByteString.copyFrom(s, "UTF-8"));
    }
    catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
    builder.setHash(HashUtils.sha256(s));
    return builder.build();
  }

  private void ensure(boolean flag) {
    if (!flag) {
      System.out.println("Assertion failed");
    }
  }

	private void go() {
      metadataStub.ping(Empty.newBuilder().build());
      logger.info("Successfully pinged the Metadata server");

      blockStub.ping(Empty.newBuilder().build());
      logger.info("Successfully pinged the Blockstore server");

      // TODO: Implement your client here

      Block b1 = stringToBlock("block-01");
      Block b2 = stringToBlock("block-02");

      //ensure(blockStub.hasBlock(b1).getAnswer() == false);
      //ensure(blockStub.hasBlock(b2).getAnswer() == false);

      blockStub.storeBlock(b1);
      ensure(blockStub.hasBlock(b1).getAnswer() == true);

      blockStub.storeBlock(b2);
      ensure(blockStub.hasBlock(b2).getAnswer() == true);

      Block b1prime = blockStub.getBlock(b1);
      ensure(b1prime.getHash().equals(b1.getHash()));
      ensure(b1prime.getData().equals(b1.getData()));

      // read a file
      // non-exist file
      FileInfo file1 = FileInfo.newBuilder().setFilename("non-exist.txt").build();
      FileInfo file1Response = metadataStub.readFile(file1);

      //FileInfo file2 = FileInfo.newBuilder().setFilename("a.txt").build();
      //FileInfo file1Response = metadataStub.readFile(file1);

      logger.info("Pass the first trial");
	}

	/*
	 * TODO: Add command line handling here
	 */
    private static Namespace parseArgs(String[] args) {
        ArgumentParser parser = ArgumentParsers.newFor("Client").build().description("Client for SurfStore");
        parser.addArgument("config_file").type(String.class).help("Path to configuration file");
        parser.addArgument("action").type(String.class).help("Client action: upload|download|delete|GetVersion");
        parser.addArgument("filename").type(String.class).help("file name");
        if (args.length == 4) {
            parser.addArgument("storage path").type(String.class).help("Path to store downloaded file");
        }

        Namespace res = null;
        try {
            res = parser.parseArgs(args);
        } catch (ArgumentParserException e){
            parser.handleError(e);
        }

        // client action
        String action = res.get("action");
        if (!action.equals("upload") && !action.equals("download") && !action.equals("delete") && !action.equals("getversion")) {
            System.out.println("Illegal action, plz use upload|download|delete|getversion");
            System.exit(2);
        }

        return res;
    }

    public static void main(String[] args) throws Exception {
        Namespace c_args = parseArgs(args);
        if (c_args == null){
            throw new RuntimeException("Argument parsing failed");
        }

        File configf = new File(c_args.getString("config_file"));
        ConfigReader config = new ConfigReader(configf);

        Client client = new Client(config);

        try {
        	client.go();
        } finally {
            client.shutdown();
        }
    }

}
