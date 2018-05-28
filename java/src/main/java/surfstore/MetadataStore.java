package surfstore;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
//import surfstore.SurfStoreBasic.Empty;

import surfstore.SurfStoreBasic.*;

import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.LinkedList;
import java.util.Arrays;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

class FileStruct {
    List<String> hashList;
    int version;
    public FileStruct(List<String> hashList, int version) {
        this.hashList = hashList;
        this.version = version;
    }
}

public final class MetadataStore {
    private static final Logger logger = Logger.getLogger(MetadataStore.class.getName());

    protected Server server;
	protected ConfigReader config;

    public MetadataStore(ConfigReader config) {
    	this.config = config;
	}

	private void start(int port, int numThreads) throws IOException {
        // add by sjt
        boolean isThisLeader = config.getLeaderPort() == port;

        server = ServerBuilder.forPort(port)
                .addService(new MetadataStoreImpl(isThisLeader, this.config))
                .executor(Executors.newFixedThreadPool(numThreads))
                .build()
                .start();
        logger.info("Server started, listening on " + port);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                System.err.println("*** shutting down gRPC server since JVM is shutting down");
                MetadataStore.this.stop();
                System.err.println("*** server shut down");
            }
        });
    }

    private void stop() {
        if (server != null) {
            server.shutdown();
        }
    }

    private void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    private static Namespace parseArgs(String[] args) {
        ArgumentParser parser = ArgumentParsers.newFor("MetadataStore").build()
                .description("MetadataStore server for SurfStore");
        parser.addArgument("config_file").type(String.class)
                .help("Path to configuration file");
        parser.addArgument("-n", "--number").type(Integer.class).setDefault(1)
                .help("Set which number this server is");
        parser.addArgument("-t", "--threads").type(Integer.class).setDefault(10)
                .help("Maximum number of concurrent threads");

        Namespace res = null;
        try {
            res = parser.parseArgs(args);
        } catch (ArgumentParserException e){
            parser.handleError(e);
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

        if (c_args.getInt("number") > config.getNumMetadataServers()) {
            throw new RuntimeException(String.format("metadata%d not in config file", c_args.getInt("number")));
        }

        final MetadataStore server = new MetadataStore(config);
        server.start(config.getMetadataPort(c_args.getInt("number")), c_args.getInt("threads"));
        server.blockUntilShutdown();
    }

    static class MetadataStoreImpl extends MetadataStoreGrpc.MetadataStoreImplBase {
        // filename -> {hash lists, version}
        protected Map<String, FileStruct> metaMap;
        protected boolean isThisLeader;

        // add by sjt
        private final ManagedChannel blockChannel;
        private final BlockStoreGrpc.BlockStoreBlockingStub blockStub;

        /*
        public MetadataStoreImpl() {
            super();
            this.metaMap = new HashMap<String, FileStruct>();
        }
        */

        public MetadataStoreImpl(boolean isThisLeader, ConfigReader config) {
            super();
            this.metaMap = new HashMap<String, FileStruct>();
            this.isThisLeader = isThisLeader;

            // add by sjt
            this.blockChannel = ManagedChannelBuilder.forAddress("127.0.0.1", config.getBlockPort()).usePlaintext(true).build();
            this.blockStub = BlockStoreGrpc.newBlockingStub(blockChannel);

        }

        @Override
        public void ping(Empty req, final StreamObserver<Empty> responseObserver) {
            Empty response = Empty.newBuilder().build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        // TODO: Implement the other RPCs!

        /**
         * <pre>
         * Read the requested file.
         * The client only needs to supply the "filename" argument of FileInfo.
         * The server only needs to fill the "version" and "blocklist" fields.
         * If the file does not exist, "version" should be set to 0.
         *
         * This command should return an error if it is called on a server
         * that is not the leader
         * </pre>
         */
         @Override
        public void readFile(surfstore.SurfStoreBasic.FileInfo request,
            io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.FileInfo> responseObserver) {
                if (!this.isThisLeader) {
                    logger.info("This is NOT a leader node");
                    System.exit(2);
                }

                String filename = request.getFilename();
                FileStruct fileStructObj = metaMap.getOrDefault(filename, new FileStruct(null, 0));

                logger.info("Read file: " + filename + "\tversion: " + fileStructObj.version);
                if (fileStructObj.hashList != null) {
                    logger.info("Hash list:");
                    for (int i = 0; i < fileStructObj.hashList.size(); i++) {
                        logger.info("\t" + fileStructObj.hashList.get(i));
                    }
                }
                else {
                    logger.info("Empty hash list");
                }

                FileInfo.Builder builder = FileInfo.newBuilder();
                builder.setVersion(fileStructObj.version);
                if (fileStructObj.hashList != null) builder.addAllBlocklist(fileStructObj.hashList);
                FileInfo response = builder.build();

                responseObserver.onNext(response);
                responseObserver.onCompleted();
        }

        /**
         * <pre>
         * Write a file.
         * The client must specify all fields of the FileInfo message.
         * The server returns the result of the operation in the "result" field.
         *
         * The server ALWAYS sets "current_version", regardless of whether
         * the command was successful. If the write succeeded, it will be the
         * version number provided by the client. Otherwise, it is set to the
         * version number in the MetadataStore.
         *
         * If the result is MISSING_BLOCKS, "missing_blocks" contains a
         * list of blocks that are not present in the BlockStore.
         *
         * This command should return an error if it is called on a server
         * that is not the leader
         * </pre>
         */
         @Override
        public void modifyFile(surfstore.SurfStoreBasic.FileInfo request,
            io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.WriteResult> responseObserver) {
                logger.info("Modify file " + request.getFilename());

                WriteResult.Builder builder = WriteResult.newBuilder();
                //builder.setFilename(request.getFilename());

                // first check if the file exists by reading
                String filename = request.getFilename();
                FileStruct fileStatOnServer = metaMap.getOrDefault(filename, new FileStruct(null, 0));

                /* modification refused */
                // not leader
                if (!this.isThisLeader) {
                    logger.info("Modification refused, not leader");
                    builder.setResult(WriteResult.Result.NOT_LEADER);
                    builder.setCurrentVersion(fileStatOnServer.version);
                }

                else if (request.getVersion() != fileStatOnServer.version + 1) {
                    logger.info("Modification refused, requested version " + request.getVersion() + " is lag behind the server (" + fileStatOnServer.version + ")");
                    builder.setResult(WriteResult.Result.OLD_VERSION);
                    builder.setCurrentVersion(fileStatOnServer.version);
                }

                // check whether missing blocks
                else {
                    // contains hash values for missing blocks
                    List<String> missingBlocks = new LinkedList<>();
                    for (String hashVal: request.getBlocklistList()) {
                        if (!blockStub.hasBlock(Block.newBuilder().setHash(hashVal).build()).getAnswer()) {
                            missingBlocks.add(hashVal);
                        }
                    }

                    // if all blocks are found in BlockStore
                    if (missingBlocks.size() <= 0) {
                        logger.info("Modificaiton verified");
                        builder.setResult(WriteResult.Result.OK);
                        builder.setCurrentVersion(request.getVersion());

                        // if OK, update metaMap mapping from filename to FileStruct
                        this.metaMap.put(filename, new FileStruct(request.getBlocklistList(), request.getVersion()));
                    }

                    // if exists missing blocks
                    else {
                        logger.info("Modification refused, missing blocks");
                        builder.setResult(WriteResult.Result.MISSING_BLOCKS);
                        builder.setCurrentVersion(fileStatOnServer.version);
                        builder.addAllMissingBlocks(missingBlocks);
                    }

                }

                WriteResult response = builder.build();
                responseObserver.onNext(response);
                responseObserver.onCompleted();
        }

        /**
         * <pre>
         * Delete a file.
         * This has the same semantics as ModifyFile, except that both the
         * client and server will not specify a blocklist or missing blocks.
         * As in ModifyFile, this call should return an error if the server
         * it is called on isn't the leader
         * </pre>
         */
         @Override
        public void deleteFile(surfstore.SurfStoreBasic.FileInfo request,
            io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.WriteResult> responseObserver) {
                logger.info("Delete file " + request.getFilename());

                WriteResult.Builder builder = WriteResult.newBuilder();

                // first check if the file exists by reading
                String filename = request.getFilename();
                FileStruct fileStatOnServer = metaMap.getOrDefault(filename, new FileStruct(null, 0));

                /* deletion refused */
                if (fileStatOnServer == null) {
                    logger.info("Deletion refused, file not found");
                    builder.setCurrentVersion(fileStatOnServer.version);
                }

                // not leader
                else if (!this.isThisLeader) {
                    logger.info("Deletion refused, not leader");
                    builder.setResult(WriteResult.Result.NOT_LEADER);
                    builder.setCurrentVersion(fileStatOnServer.version);
                }

                else if (request.getVersion() != fileStatOnServer.version + 1) {
                    logger.info("Deletion refused, requested version " + request.getVersion() + " is lag behind the server (" + fileStatOnServer.version + ")");
                    builder.setResult(WriteResult.Result.OLD_VERSION);
                    builder.setCurrentVersion(fileStatOnServer.version);
                }

                // delete file
                else {
                    logger.info("Deletion verified");
                    builder.setResult(WriteResult.Result.OK);
                    builder.setCurrentVersion(request.getVersion());

                    // set hashlist as {"0",}
                    this.metaMap.put(filename, new FileStruct(new LinkedList<String>(Arrays.asList("0")), request.getVersion()));
                }

                WriteResult response = builder.build();
                responseObserver.onNext(response);
                responseObserver.onCompleted();
        }

        /**
         * <pre>
         * Query whether the MetadataStore server is currently the leader.
         * This call should work even when the server is in a "crashed" state
         * </pre>
         */
         @Override
        public void isLeader(surfstore.SurfStoreBasic.Empty request,
            io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.SimpleAnswer> responseObserver) {
                logger.info("Testing if it's the leader: " + this.isThisLeader);

                SimpleAnswer response = SimpleAnswer.newBuilder().setAnswer(this.isThisLeader).build();
                responseObserver.onNext(response);
                responseObserver.onCompleted();
        }

        /**
         * <pre>
         * "Crash" the MetadataStore server.
         * Until Restore() is called, the server should reply to all RPCs
         * with an error (except Restore) and not send any RPCs to other servers.
         * </pre>
         */
         @Override
        public void crash(surfstore.SurfStoreBasic.Empty request,
            io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.Empty> responseObserver) {
                Empty response = Empty.newBuilder().build();
                responseObserver.onNext(response);
                responseObserver.onCompleted();
        }

        /**
         * <pre>
         * "Restore" the MetadataStore server, allowing it to start
         * sending and responding to all RPCs once again.
         * </pre>
         */
         @Override
        public void restore(surfstore.SurfStoreBasic.Empty request,
            io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.Empty> responseObserver) {
                Empty response = Empty.newBuilder().build();
                responseObserver.onNext(response);
                responseObserver.onCompleted();
        }

        /**
         * <pre>
         * Find out if the node is crashed or not
         * (should always work, even if the node is crashed)
         * </pre>
         */
         @Override
        public void isCrashed(surfstore.SurfStoreBasic.Empty request,
            io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.SimpleAnswer> responseObserver) {
                SimpleAnswer response = SimpleAnswer.newBuilder().setAnswer(false).build();
                responseObserver.onNext(response);
                responseObserver.onCompleted();
        }

        /**
         * <pre>
         * Returns the current committed version of the requested file
         * The argument's FileInfo only has the "filename" field defined
         * The FileInfo returns the filename and version fields only
         * This should return a result even if the follower is in a
         *   crashed state
         * </pre>
         */
         @Override
        public void getVersion(surfstore.SurfStoreBasic.FileInfo request,
            io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.FileInfo> responseObserver) {
                FileStruct fileStructObj = this.metaMap.getOrDefault(request.getFilename(), new FileStruct(null, 0));
                // if file is deleted at meta server, also return not found
                if (fileStructObj.hashList.size() == 1 && fileStructObj.hashList.get(0).equals("0")) {
                    fileStructObj = new FileStruct(null, 0);
                }

                FileInfo.Builder builder = FileInfo.newBuilder();
                builder.setFilename(request.getFilename());
                builder.setVersion(fileStructObj.version);

                logger.info("Get version of file " + request.getFilename() + " version is " + fileStructObj.version);

                FileInfo response = builder.build();
                responseObserver.onNext(response);
                responseObserver.onCompleted();
        }

    }
}
