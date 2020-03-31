package com.emc.ecs.utils;

import com.emc.ecs.management.sdk.ObjectUserMapAction;
import com.emc.ecs.servicebroker.config.BrokerConfig;
import com.emc.object.s3.S3Client;
import com.emc.object.s3.S3Config;
import com.emc.object.s3.bean.AbstractVersion;
import com.emc.object.s3.bean.DeleteObjectsResult;
import com.emc.object.s3.bean.EncodingType;
import com.emc.object.s3.bean.ListObjectsResult;
import com.emc.object.s3.bean.ListVersionsResult;
import com.emc.object.s3.bean.S3Object;
import com.emc.object.s3.jersey.S3JerseyClient;
import com.emc.object.s3.request.DeleteObjectsRequest;
import com.emc.object.s3.request.ListObjectsRequest;
import com.emc.object.s3.request.ListVersionsRequest;
import com.emc.object.util.RestUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import javax.annotation.PostConstruct;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicLong;

public class BucketWipe {
    public static final int DEFAULT_THREADS = 32;
    public static final int QUEUE_SIZE = 2000;

    static final Logger LOG = LoggerFactory.getLogger(BucketWipe.class);

    private int threads = DEFAULT_THREADS;
    private boolean keepBucket;
    private EnhancedThreadPoolExecutor executor;
    private List<String> errors = Collections.synchronizedList(new ArrayList<String>());
    private String lastKey;
    private boolean hierarchical;


    private S3JerseyClient s3Client;

    @Autowired
    private BrokerConfig broker;

    public void BucketWipe() {
    }

    public void initialize() throws URISyntaxException {
        S3Config s3Config = new S3Config(new URI(broker.getRepositoryEndpoint()));
        s3Config.withIdentity(broker.getPrefixedUserName())
            .withSecretKey(broker.getRepositorySecret());
        this.s3Client = new S3JerseyClient(s3Config);

        executor = new EnhancedThreadPoolExecutor(threads, new LinkedBlockingDeque<Runnable>(QUEUE_SIZE));

        LOG.info("Bucket wipe Initialized");
    }

    public void stop() {
        executor.shutdownNow();
    }

    public void deleteAllObjects(String bucket) {
        if (s3Client.getBucketVersioning(bucket).getStatus() == null) {
            deleteAllObjects(bucket, "");
        } else {
            deleteAllVersions(bucket, "");
        }
    }

    protected void deleteAllObjects(String bucket, String prefix) {
        LOG.info("deleting all objects in bucket {}", bucket);

        AtomicLong deletedObjects = new AtomicLong(0);
        List<Future> futures = new ArrayList<>();
        ListObjectsResult listing = null;
        ListObjectsRequest request = new ListObjectsRequest(bucket).withPrefix(prefix).withEncodingType(EncodingType.url);
        do {
            if (listing == null) listing = s3Client.listObjects(request);
            else listing = s3Client.listMoreObjects(listing);

            for (S3Object object : listing.getObjects()) {
                lastKey = object.getKey();
                futures.add(executor.blockingSubmit(new DeleteObjectTask(s3Client, bucket, object.getKey())));
            }

            while (futures.size() > QUEUE_SIZE) {
                handleSingleFutures(futures, QUEUE_SIZE / 2, deletedObjects);
            }
        } while (listing.isTruncated());

        handleSingleFutures(futures, futures.size(), deletedObjects);

        LOG.info("Deleted {} objects", deletedObjects.get());
    }

    protected void deleteAllVersions(String bucket, String prefix) {
        LOG.info("deleting all versions in bucket {}", bucket);

        AtomicLong deletedObjects = new AtomicLong(0);
        List<Future> futures = new ArrayList<>();
        ListVersionsResult listing = null;
        ListVersionsRequest request = new ListVersionsRequest(bucket).withPrefix(prefix).withEncodingType(EncodingType.url);
        do {
            if (listing != null) {
                request.setKeyMarker(listing.getNextKeyMarker());
                request.setVersionIdMarker(listing.getNextVersionIdMarker());
            }
            listing = s3Client.listVersions(request);

            for (AbstractVersion version : listing.getVersions()) {
                lastKey = version.getKey() + " (version " + version.getVersionId() + ")";
                futures.add(executor.blockingSubmit(new DeleteVersionTask(s3Client, bucket,
                        RestUtil.urlDecode(version.getKey()), version.getVersionId())));
            }

            while (futures.size() > QUEUE_SIZE) {
                handleSingleFutures(futures, QUEUE_SIZE / 2, deletedObjects);
            }
        } while (listing.isTruncated());

        handleSingleFutures(futures, futures.size(),deletedObjects);
        LOG.info("Deleted {} versions",deletedObjects.get());
    }

    protected void handleSingleFutures(List<Future> futures, int num, AtomicLong deletedObjects) {
        for (Iterator<Future> i = futures.iterator(); i.hasNext() && num-- > 0; ) {
            Future future = i.next();
            i.remove();
            try {
                future.get();
                deletedObjects.incrementAndGet();
            } catch (InterruptedException e) {
                errors.add(e.getMessage());
            } catch (ExecutionException e) {
                errors.add(e.getCause().getMessage());
            }
        }
    }

    public List<String> getErrors() {
        return errors;
    }

    public int getThreads() {
        return threads;
    }

    public void setThreads(int threads) {
        this.threads = threads;
    }

    public boolean isKeepBucket() {
        return keepBucket;
    }

    public void setKeepBucket(boolean keepBucket) {
        this.keepBucket = keepBucket;
    }

    public boolean isHierarchical() {
        return hierarchical;
    }

    public void setHierarchical(boolean hierarchical) {
        this.hierarchical = hierarchical;
    }

    public String getLastKey() {
        return lastKey;
    }


    public BucketWipe withThreads(int threads) {
        setThreads(threads);
        return this;
    }

    public BucketWipe withKeepBucket(boolean keepBucket) {
        setKeepBucket(keepBucket);
        return this;
    }

    protected class DeleteBatchObjectsTask implements Callable<DeleteObjectsResult> {
        private S3Client client;
        private DeleteObjectsRequest request;

        public DeleteBatchObjectsTask(S3Client client, DeleteObjectsRequest request) {
            this.client = client;
            this.request = request;
        }

        @Override
        public DeleteObjectsResult call() {
            return client.deleteObjects(request);
        }
    }

    protected class DeleteObjectTask implements Runnable {
        private S3Client client;
        private String bucket;
        private String key;

        public DeleteObjectTask(S3Client client, String bucket, String key) {
            this.client = client;
            this.bucket = bucket;
            this.key = key;
        }

        @Override
        public void run() {
            client.deleteObject(bucket, key);
        }
    }

    protected class DeleteVersionTask implements Runnable {
        private S3Client client;
        private String bucket;
        private String key;
        private String versionId;

        public DeleteVersionTask(S3Client client, String bucket, String key, String versionId) {
            this.client = client;
            this.bucket = bucket;
            this.key = key;
            this.versionId = versionId;
        }

        @Override
        public void run() {
            client.deleteVersion(bucket, key, versionId);
        }
    }
}
