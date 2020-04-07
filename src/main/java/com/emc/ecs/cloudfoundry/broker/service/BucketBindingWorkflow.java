package com.emc.ecs.cloudfoundry.broker.service;

import com.emc.ecs.cloudfoundry.broker.EcsManagementClientException;
import com.emc.ecs.cloudfoundry.broker.repository.ServiceInstance;
import com.emc.ecs.cloudfoundry.broker.repository.ServiceInstanceBinding;
import com.emc.ecs.cloudfoundry.broker.repository.ServiceInstanceRepository;
import com.emc.ecs.management.sdk.model.UserSecretKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.servicebroker.exception.ServiceInstanceBindingExistsException;
import org.springframework.cloud.servicebroker.exception.ServiceInstanceDoesNotExistException;
import org.springframework.cloud.servicebroker.model.CreateServiceInstanceAppBindingResponse;
import org.springframework.cloud.servicebroker.model.SharedVolumeDevice;
import org.springframework.cloud.servicebroker.model.VolumeMount;

import javax.xml.bind.JAXBException;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.*;

public class BucketBindingWorkflow extends BindingWorkflowImpl {
    private static final String VOLUME_DRIVER = "nfsv3driver";
    private static final String DEFAULT_MOUNT = "/var/vcap/data";
    private static final String MOUNT = "mount";
    private List<VolumeMount> volumeMounts;
    private static final Logger LOG =
            LoggerFactory.getLogger(EcsServiceInstanceBindingService.class);

    BucketBindingWorkflow(ServiceInstanceRepository instanceRepo, EcsService ecs) throws IOException {
        super(instanceRepo, ecs);
    }

    public void checkIfUserExists() throws EcsManagementClientException, IOException {
        if (ecs.userExists(bindingId, instanceName))
            throw new ServiceInstanceBindingExistsException(instanceId, bindingId);
    }

    @Override
    public String createBindingUser() throws EcsManagementClientException, IOException, JAXBException {
        ServiceInstance instance = instanceRepository.find(instanceId);
        if (instance == null) {
            throw new ServiceInstanceDoesNotExistException(instanceId);
        }

        Map<String, Object> parameters = createRequest.getParameters();
        instanceName = ecs.getInstanceName(instance.getServiceSettings());
        UserSecretKey userSecretKey = ecs.createUser(bindingId, instanceName);

        if (instance.getName() == null)
            instance.setName(instance.getServiceInstanceId());
        String bucketId = instance.getName();

        String export = "";
        List<String> permissions = null;
        if (parameters != null) {
            permissions = (List<String>) parameters.get("permissions");
            export = (String) parameters.getOrDefault("export", null);
        }

        if (permissions == null) {
            ecs.addUserToBucket(bucketId, bindingId, instanceName);
        } else {
            ecs.addUserToBucket(bucketId, bindingId, permissions, instanceName);
        }

        if (ecs.getBucketFileEnabled(bucketId, instanceName)) {
            volumeMounts = createVolumeExport(export,
                    new URL(ecs.getObjectEndpoint()), parameters);
        }

        return userSecretKey.getSecretKey();
    }

    @Override
    public void removeBinding(ServiceInstanceBinding binding)
            throws EcsManagementClientException, IOException {
        ServiceInstance instance = instanceRepository.find(instanceId);
        if (instance == null)
            throw new ServiceInstanceDoesNotExistException(instanceId);

        instanceName = ecs.getInstanceName(instance.getServiceSettings());

        if (instance.getName() == null)
            instance.setName(instance.getServiceInstanceId());
        String bucketName = instance.getName();

        List<VolumeMount> volumes = binding.getVolumeMounts();
        if (volumes != null && volumes.size() > 0) {
            Map<String, Object> mountConfig = (
                        (SharedVolumeDevice) volumes.get(0).getDevice()
                    ).getMountConfig();
            String unixId = (String) mountConfig.get("uid");
            LOG.error("Deleting user map of instance Id and Binding Id " +
                    bucketName + " " + bindingId);
            try {
                ecs.deleteUserMap(bindingId, unixId, instanceName);
            } catch (EcsManagementClientException e) {
                LOG.error("Error deleting user map: " + e.getMessage());
            }
        }

        ecs.removeUserFromBucket(bucketName, bindingId, instanceName);
        ecs.deleteUser(bindingId, instanceName);
    }

    @Override
    public Map<String, Object> getCredentials(String secretKey, Map<String, Object> parameters)
            throws IOException, EcsManagementClientException {
        ServiceInstance instance = instanceRepository.find(instanceId);
        if (instance == null)
            throw new ServiceInstanceDoesNotExistException(instanceId);

        if (instance.getName() == null)
            instance.setName(instance.getServiceInstanceId());
        String bucketName = instance.getName();

        Map<String, Object> credentials = super.getCredentials(secretKey);

        // Add default broker endpoint
        String endpoint = ecs.getObjectEndpoint();
        credentials.put("endpoint", endpoint);

        // Add s3 URL
        URL baseUrl = new URL(endpoint);
        credentials.put("s3Url", getS3Url(baseUrl, secretKey, parameters));

        if (parameters != null && parameters.containsKey("path-style-access") &&
                ! (Boolean) parameters.get("path-style-access"))
        {
            credentials.put("path-style-access", false);
        } else {
            credentials.put("path-style-access", true);
        }

        // Add bucket name from repository
        credentials.put("bucket", ecs.prefix(bucketName, instanceName));

        return credentials;
    }

    @Override
    public ServiceInstanceBinding getBinding(Map<String, Object> credentials) {
        ServiceInstanceBinding binding = new ServiceInstanceBinding(createRequest);
        binding.setBindingId(bindingId);
        binding.setCredentials(credentials);
        if (volumeMounts != null)
            binding.setVolumeMounts(volumeMounts);
        return binding;
    }

    @Override
    public CreateServiceInstanceAppBindingResponse getResponse(
            Map<String, Object> credentials) {
        CreateServiceInstanceAppBindingResponse resp =
                new CreateServiceInstanceAppBindingResponse()
                .withCredentials(credentials);
        if (volumeMounts != null)
            resp = resp.withVolumeMounts(volumeMounts);

        return resp;
    }

    private String getS3Url(URL baseUrl, String secretKey, Map<String, Object> parameters) {
        String userInfo = getUserInfo(secretKey);
        String s3Url = baseUrl.getProtocol() + "://" + userInfo + "@";

        String portString = "";
        if (baseUrl.getPort() != -1)
            portString = ":" + baseUrl.getPort();

        if (parameters != null && parameters.containsKey("path-style-access") &&
                ! (Boolean) parameters.get("path-style-access"))
        {
            s3Url = s3Url + ecs.prefix(instanceId, instanceName) + "." + baseUrl.getHost() + portString;
        } else {
            s3Url = s3Url + baseUrl.getHost() + portString + "/" + ecs.prefix(instanceId, instanceName);
        }
        return s3Url;
    }

    private int createUserMap() throws EcsManagementClientException {
        int unixUid = (int) (2000 + System.currentTimeMillis() % 8000);
        while (true) {
            try {
                ecs.createUserMap(bindingId, unixUid, instanceName);
                break;
            } catch (EcsManagementClientException e) {
                if (e.getMessage().contains("Bad request body (1013)")) {
                    unixUid++;
                } else {
                    throw e;
                }
            }
        }
        return unixUid;
    }

    private List<VolumeMount> createVolumeExport(String export, URL baseUrl, Map<String, Object>parameters)
            throws EcsManagementClientException {
        int unixUid = createUserMap();
        String host = ecs.getNfsMountHost();
        if (host == null || host.isEmpty()) {
            host = baseUrl.getHost();
        }

        LOG.info("Adding export:  " + export + " to bucket: " + instanceId);
        String volumeGUID = UUID.randomUUID().toString();
        String absoluteExportPath = ecs.addExportToBucket(instanceId, export, instanceName);
        LOG.info("export added.");

        Map<String, Object> opts = new HashMap<>();
        String nfsUrl = "nfs://" + host + absoluteExportPath;

        opts.put("source", nfsUrl);
        opts.put("uid", String.valueOf(unixUid));

        List<VolumeMount> mounts = new ArrayList<>();
        mounts.add(new VolumeMount(VOLUME_DRIVER, getContainerDir(parameters, bindingId),
                VolumeMount.Mode.READ_WRITE, VolumeMount.DeviceType.SHARED,
                new SharedVolumeDevice(volumeGUID, opts)));

        return mounts;
    }

    private String getContainerDir(Map<String, Object> parameters, String bindingId) {
        if (parameters != null) {
            Object o = parameters.get(MOUNT);
            if (o != null && o instanceof String) {
                String mount = (String) o;
                if (!mount.isEmpty()) {
                    return mount;
                }
            }
        }
        return DEFAULT_MOUNT + "/" + bindingId;
    }
}
