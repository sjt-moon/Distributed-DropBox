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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.LinkedList;
import java.util.Arrays;

import java.nio.file.StandardOpenOption;
import java.util.Scanner;

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

    /* **************
    * core functions
    * ***************
    */

    private boolean upload (String pathToFile) throws IOException {
        // 1st: read file checking existence
        File f = new File(pathToFile);
        // if file does not exist
        if (!(f.exists() && !f.isDirectory())) {
            System.out.println("Not Found");
            return false;
        }

        // extract filename from pathToFile
        String[] directory = pathToFile.split("/");
        String filename = directory[directory.length - 1];

        FileInfo fileReadRequest = FileInfo.newBuilder().setFilename(filename).build();
        FileInfo fileReadResponse = metadataStub.readFile(fileReadRequest);
        int writeVersion = fileReadResponse.getVersion() + 1;

        // if file does exist, return version number
        if (fileReadResponse.getVersion() != 0) {
            logger.info("File " + filename + " already exists with version " + fileReadResponse.getVersion() + ". Continue to update the file");
        }

        // 2nd: check missing blocks @ meta server
        FileInfo.Builder fileModifyBuilder = FileInfo.newBuilder();
        fileModifyBuilder.setFilename(filename);
        fileModifyBuilder.setVersion(writeVersion);
        Map<String, ByteString> hashDataMap = getHashDataMap(filename);
        fileModifyBuilder.addAllBlocklist(hashDataMap.keySet());

        FileInfo fileModifyRequest = fileModifyBuilder.build();
        WriteResult fileModifyResponse = metadataStub.modifyFile(fileModifyRequest);

        for (int i = 0; i < 2; i++) {
            // if all are in BlockStore, upload success
            if (fileModifyResponse.getResult() == WriteResult.Result.OK) {
                logger.info("Upload done");
                return true;
            }
            else if (fileModifyResponse.getResult() == WriteResult.Result.OLD_VERSION || fileModifyResponse.getResult() == WriteResult.Result.NOT_LEADER) {
                logger.info("Upload failed, old version or not to a leader");
                return false;
            }
            else {
                // contains hash values for missing blocks
                logger.info("Upload failed, missing blocks");

                List<String> missingBlocks = fileModifyResponse.getMissingBlocksList();
                for (String hashVal: missingBlocks) {
                    Block.Builder missingBlockBuilder = Block.newBuilder();
                    missingBlockBuilder.setHash(hashVal);
                    missingBlockBuilder.setData(hashDataMap.get(hashVal));
                    this.blockStub.storeBlock(missingBlockBuilder.build());
                }

                // then request modifyFile again
                fileModifyResponse = metadataStub.modifyFile(fileModifyRequest);
            }
        }
        System.out.println("OK");
        return true;
    }

    private boolean download(String filename) throws IOException {
        return download(filename, ".");
    }

    private boolean download(String filename, String pathBeforeFile) throws IOException {
        String pathToFile = pathBeforeFile + "/" + filename;

        ByteString content = ByteString.copyFrom("", "UTF-8");

        // 1st: check if exists on meta server, record hashlist @ server
        FileInfo fileDownloadRequest = FileInfo.newBuilder().setFilename(filename).build();
        FileInfo fileDownloadResponse = this.metadataStub.readFile(fileDownloadRequest);

        // if does not exist
        boolean isDeletedOnServer = fileDownloadResponse.getBlocklistList().size() == 1 && fileDownloadResponse.getBlocklistList().get(0).equals("0");
        if (fileDownloadResponse.getVersion() == 0 || isDeletedOnServer) {
            System.out.println("Not Found");
            return false;
        }

        List<String> hashListOnServer = fileDownloadResponse.getBlocklistList();

        // 2nd: get hashlist of local copy of that file
        Map<String, ByteString> hashDataMapOnLocal = getHashDataMap(filename);

        for (String hashVal: hashListOnServer) {
            if (!hashDataMapOnLocal.containsKey(hashVal)) {
                Block missingBlock = Block.newBuilder().setHash(hashVal).build();
                Block downloadBlock = this.blockStub.getBlock(missingBlock);

                //content += downloadBlock.getData().toString();
                content = content.concat(downloadBlock.getData());
            }
            else {
                //content += hashDataMapOnLocal.get(hashVal).toString();
                content = content.concat(hashDataMapOnLocal.get(hashVal));
            }
        }

        // over-write to file
        // if file does not exist locally, use StandardOpenOption.CREATE
        File fr = new File(pathToFile);
        if (!fr.exists()) {
            Files.write(Paths.get(pathToFile), content.toByteArray(), StandardOpenOption.CREATE);
        }
        else {
            Files.write(Paths.get(pathToFile), content.toByteArray(), StandardOpenOption.TRUNCATE_EXISTING);
        }

        System.out.println("OK");
        return true;
    }

    private boolean delete(String filename) {
        // 1st: read file checking existence
        FileInfo fileReadRequest = FileInfo.newBuilder().setFilename(filename).build();
        FileInfo fileReadResponse = metadataStub.readFile(fileReadRequest);
        int writeVersion = fileReadResponse.getVersion() + 1;

        // 2nd: delete file on the server
        FileInfo fileDeleteRequest = FileInfo.newBuilder().setFilename(filename).setVersion(writeVersion).addAllBlocklist(new LinkedList<String>(Arrays.asList("0"))).build();
        WriteResult fileDeleteResponse = this.metadataStub.deleteFile(fileDeleteRequest);

        if (fileDeleteResponse.getResult() == WriteResult.Result.OK) {
            System.out.println("OK");
            return true;
        }
        return false;
    }

    private int getVersion(String filename) {
        FileInfo getVersionRequest = FileInfo.newBuilder().setFilename(filename).build();
        FileInfo getVersionResponse = this.metadataStub.getVersion(getVersionRequest);

        // if file not found or deleted
        int version = getVersionResponse.getVersion();
        // if (version == 0) {
        //     System.out.println("Not Found");
        //     //return false;
        // }

        String versionText = "" + version;
        for (int followerVersion: getVersionResponse.getFollowerVersionsList()) {
            versionText += " " + followerVersion;
        }
        System.out.println(versionText);

        //System.out.println(getVersionResponse.getVersion());
        //return true;
        return version;
    }

    private void go(Namespace args) throws IOException {
      metadataStub.ping(Empty.newBuilder().build());
      logger.info("Successfully pinged the Metadata server");

      blockStub.ping(Empty.newBuilder().build());
      logger.info("Successfully pinged the Blockstore server");

      // TODO: Implement your client here

      /*
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
      */

      /***********************************
      * unit test on centralized scenario
      ************************************/

      /*
      readNonExistFile();
      readExistFile();

      uploadNonExistFile();
      uploadExistFile();
      uploadDuplicateFile();

      downloadNonExistFile();
      downloadExistFile();
      downloadModifiedFile();
      */

      // crashAndRecoverOneServer();

      String action = args.getString("action").toLowerCase();
      String filename = args.getString("filename");

      if (action.equals("upload")) {
          upload(filename);
      }
      else if (action.equals("download")) {
          String storagePath = args.getString("storage path");
          download(filename, storagePath);
      }
      else if (action.equals("delete")) {
          delete(filename);
      }
      else if (action.equals("getversion")) {
          getVersion(filename);
      }
      else {
          logger.info("Unrecognized action, acceptable actions are upload|download|delete|getversion");
      }

      logger.info("Pass the first trial");
	}

	/*
	 * TODO: Add command line handling here
	 */
    private static Namespace parseArgs(String[] args) {
        ArgumentParser parser = ArgumentParsers.newFor("Client").build().description("Client for SurfStore");
        parser.addArgument("config_file").type(String.class).help("Path to configuration file");
        parser.addArgument("action").type(String.class).help("Client action: upload|download|delete|getversion");
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
        	client.go(c_args);
        } finally {
            client.shutdown();
        }
    }

    /* **********
    * unit tests
    * centralized
    * ***********
    */

    // read a file which is NOT in the server
    public void readNonExistFile() throws IOException {
        FileInfo file = FileInfo.newBuilder().setFilename("non-exist.txt").build();
        FileInfo fileResponse = metadataStub.readFile(file);

        ensure(fileResponse.getVersion() == 0, "Assertion failed: read non-exist file, readFile()");
    }

    // read a file which is stored in metadata server
    public void readExistFile() throws IOException {
        upload("a.txt");

        FileInfo file = FileInfo.newBuilder().setFilename("a.txt").build();
        FileInfo fileResponse = metadataStub.readFile(file);

        ensure(fileResponse.getVersion() > 0, "Assertion failed: read exist file, readFile()");
    }

    // upload a file which is NOT FOUND locally
    public void uploadNonExistFile() throws IOException {
        ensure(!upload("non-exist.txt"), "Assertion failed: upload non-exist file, upload()");
    }

    // upload a file from local client
    public void uploadExistFile() throws IOException {
        ensure(upload("a.txt"), "Assertion failed: upload exist file, upload()");
    }

    // upload a duplicate file from local client to server
    public void uploadDuplicateFile() throws IOException {
        boolean flag = upload("a.txt");
        flag &= upload("a.txt");
        ensure(flag, "Assertion failed: upload duplicate files, upload()");
    }

    // download a file which is NOT FOUND at server
    public void downloadNonExistFile() throws IOException {
        ensure(!download("non-exist.txt"), "Assertion failed: download non-exist file, download()");
    }

    // download a file from server, and that file is NOT FOUND at local
    public void downloadExistFile() throws IOException {
        upload("a.txt");

        // remove local file
        File file = new File("a.txt");
        file.delete();
        showFileContent("a.txt", "Delete local file: ");

        download("a.txt");
        showFileContent("a.txt", "Downloaded file content: ");
    }

    public void downloadModifiedFile() throws IOException {
        // 1st: print out a.txt content
        showFileContent("a.txt", "Original file content is:");

        // 2nd: modify a.txt locally
        Files.write(Paths.get("a.txt"), "the text".getBytes(), StandardOpenOption.TRUNCATE_EXISTING);
        showFileContent("a.txt", "Now changed the file locally as follows:");

        // 3rd: download from server
        ensure(download("a.txt"), "Assertion failed: error at download()");

        // 4th: show contents after downloading from server
        showFileContent("a.txt", "After downloading from server, file content is:");
    }

    /* **********
    * unit tests
    * distributed
    * ***********
    */

    /* when one follower meta server crashes & recovers
    * story line:
    * - upload files
    * - crash server #2
    * - keep uploading
    * - get versions
    * - recover server # 2
    * - sleep 5 seconds
    * - get versions // heartbeats would update lagged servers
    */
    public void crashAndRecoverOneServer() throws IOException {
        upload("a.txt");

        // crash server #2 (follower)
        // first get connected to that server
        ManagedChannel serverCrashedChannel;
        MetadataStoreGrpc.MetadataStoreBlockingStub serverCrashedStub;
        serverCrashedChannel = ManagedChannelBuilder.forAddress("127.0.0.1", config.getMetadataPort(2)).usePlaintext(true).build();
        serverCrashedStub = MetadataStoreGrpc.newBlockingStub(serverCrashedChannel);

        serverCrashedStub.crash(Empty.newBuilder().build());

        upload("a.txt");

        this.getVersion("a.txt");

        // recover server #2
        serverCrashedStub.restore(Empty.newBuilder().build());

        try {
            Thread.sleep(3000);
        }
        catch (InterruptedException e) {
            System.out.println("Client sleep thread error @ crashAndRecoverOneServer()");
        }

        this.getVersion("a.txt");
    }

    /* **************
    * some utilities
    * ***************
    */
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

    /* hash -> ByteString */
    private Map<String, ByteString> getHashDataMap(String filename) throws IOException {
        Map<String, ByteString> map = new HashMap<>();

        // if file does not exist
        File f = new File(filename);
        if (!(f.exists() && !f.isDirectory())) {
            return map;
        }

        String content = new String(Files.readAllBytes(Paths.get(filename)));

        int numBlocks = content.length() / 4096 + content.length() % 4096 == 0 ? 0 : 1;
        for (int i = 0; i < numBlocks; i++) {
            String stringBlock = content.substring(i * 4096, (i + 1) * 4096 < content.length() ? (i + 1) * 4096 : content.length());
            ByteString byteBlock = ByteString.copyFrom(stringBlock, "UTF-8");
            String hashVal = HashUtils.sha256(stringBlock);
            map.put(hashVal, byteBlock);
        }
        return map;
    }

    private void ensure(boolean flag) {
        if (!flag) {
            System.out.println("Assertion failed");
        }
    }

    private void ensure(boolean flag, String errorMsg) {
        if (!flag) {
            System.out.println("Assertion failed, " + errorMsg);
        }
    }

    public void showFileContent(String filename, String explaination) throws IOException {
        System.out.println(explaination);

        File fr = new File(filename);
        if (!fr.exists() || fr.isDirectory()) {
            System.out.println("File " + filename + " does not exist or its a directory");
            return;
        }

        Scanner sc = new Scanner(fr);
        while (sc.hasNextLine()) {
            System.out.println("----> " + sc.nextLine());
        }
    }
}
