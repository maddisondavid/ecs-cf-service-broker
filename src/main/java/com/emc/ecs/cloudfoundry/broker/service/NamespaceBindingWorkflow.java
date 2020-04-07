package com.emc.ecs.cloudfoundry.broker.service;

import com.emc.ecs.cloudfoundry.broker.EcsManagementClientException;
import com.emc.ecs.cloudfoundry.broker.repository.ServiceInstance;
import com.emc.ecs.cloudfoundry.broker.repository.ServiceInstanceBinding;
import com.emc.ecs.cloudfoundry.broker.repository.ServiceInstanceRepository;
import org.springframework.cloud.servicebroker.exception.ServiceInstanceBindingExistsException;
import org.springframework.cloud.servicebroker.exception.ServiceInstanceDoesNotExistException;
import org.springframework.cloud.servicebroker.model.CreateServiceInstanceAppBindingResponse;

import javax.xml.bind.JAXBException;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;

public class NamespaceBindingWorkflow extends BindingWorkflowImpl {

    NamespaceBindingWorkflow(ServiceInstanceRepository instanceRepo, EcsService ecs) throws IOException {
        super(instanceRepo, ecs);
    }

    public void checkIfUserExists() throws EcsManagementClientException, IOException {
        if (ecs.userExists(bindingId, instanceName))
            throw new ServiceInstanceBindingExistsException(instanceId, bindingId);
    }

    @Override
    public String createBindingUser() throws EcsManagementClientException, IOException, JAXBException {
        ServiceInstance instance = instanceRepository.find(instanceId);
        if (instance == null)
            throw new ServiceInstanceDoesNotExistException(instanceId);

        if (instance.getName() == null)
            instance.setName(instance.getServiceInstanceId());
        String namespaceName = instance.getName();

        return ecs.createUser(bindingId, namespaceName, instanceName).getSecretKey();
    }

    @Override
    public void removeBinding(ServiceInstanceBinding binding) throws EcsManagementClientException {
        instanceName = ecs.getInstanceName(binding.getParameters());

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
        String namespaceName = instance.getName();

        Map<String, Object> credentials = super.getCredentials(secretKey);

        // Get custom endpoint for namespace
        String endpoint = ecs.getNamespaceURL(ecs.prefix(namespaceName, instanceName), service, plan,
                createRequest.getParameters());
        credentials.put("endpoint", endpoint);

        // Add s3 URL
        credentials.put("s3Url", getS3Url(endpoint, secretKey));

        return credentials;
    }

    @Override
    public CreateServiceInstanceAppBindingResponse getResponse(Map<String, Object> credentials) {
        return new CreateServiceInstanceAppBindingResponse()
                .withCredentials(credentials);
    }

    private String getS3Url(String endpoint, String secretKey) throws MalformedURLException {
        URL baseUrl = new URL(endpoint);
        String userInfo = getUserInfo(secretKey);
        return baseUrl.getProtocol() + "://" + userInfo + "@" +
                baseUrl.getHost() + ":" + baseUrl.getPort();
    }
}
