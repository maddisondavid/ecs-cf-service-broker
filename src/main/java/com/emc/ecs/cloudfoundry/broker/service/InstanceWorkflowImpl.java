package com.emc.ecs.cloudfoundry.broker.service;

import com.emc.ecs.cloudfoundry.broker.repository.ServiceInstance;
import com.emc.ecs.cloudfoundry.broker.repository.ServiceInstanceRepository;
import org.springframework.cloud.servicebroker.model.CreateServiceInstanceRequest;
import org.springframework.cloud.servicebroker.model.DeleteServiceInstanceRequest;

import java.util.Map;
import java.util.Optional;

abstract public class InstanceWorkflowImpl implements InstanceWorkflow {
    protected final EcsService ecs;
    final ServiceInstanceRepository instanceRepository;
    String instanceId;
    String instanceName = "";
    CreateServiceInstanceRequest createRequest;

    InstanceWorkflowImpl(ServiceInstanceRepository instanceRepo, EcsService ecs) {
        this.instanceRepository = instanceRepo;
        this.ecs = ecs;
    }

    public InstanceWorkflow withCreateRequest(CreateServiceInstanceRequest request) {
        this.instanceId = request.getServiceInstanceId();
        this.createRequest = request;
        this.instanceName = ecs.getInstanceName(request.getParameters());
        return(this);
    }

    public InstanceWorkflow withDeleteRequest(DeleteServiceInstanceRequest request) {
        this.instanceId = request.getServiceInstanceId();
        return(this);
    }

    ServiceInstance getServiceInstance(Map<String, Object> serviceSettings) {
        ServiceInstance instance = new ServiceInstance(createRequest);
        instance.setServiceSettings(serviceSettings);
        return instance;
    }

}
