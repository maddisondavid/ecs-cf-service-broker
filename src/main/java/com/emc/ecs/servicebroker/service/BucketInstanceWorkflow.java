package com.emc.ecs.servicebroker.service;

import com.emc.ecs.servicebroker.config.BrokerConfig;
import com.emc.ecs.servicebroker.model.PlanProxy;
import com.emc.ecs.servicebroker.model.ServiceDefinitionProxy;
import com.emc.ecs.servicebroker.repository.ServiceInstance;
import com.emc.ecs.servicebroker.repository.ServiceInstanceRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.servicebroker.exception.ServiceBrokerException;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class BucketInstanceWorkflow extends InstanceWorkflowImpl {
    @Autowired
    private BrokerConfig broker;

    BucketInstanceWorkflow(ServiceInstanceRepository repo, EcsService ecs) {
        super(repo, ecs);
    }

    @Override
    public Map<String, Object> changePlan(String id, ServiceDefinitionProxy service, PlanProxy plan, Map<String, Object> parameters) {
        return ecs.changeBucketPlan(id, service, plan, parameters);
    }

    @Override
    public void delete(String id) {
        try {
            ServiceInstance instance = instanceRepository.find(id);
            if (instance.getReferences().size() > 1) {
                removeInstanceFromReferences(instance, id);
            } else {
                ecs.addBrokerUserToBucket(id);
                ecs.wipeBucket(id);
                ecs.deleteBucket(id);
            }
        } catch (IOException e) {
            throw new ServiceBrokerException(e);
        }
    }

    private void removeInstanceFromReferences(ServiceInstance instance, String id) throws IOException {
        for (String refId : instance.getReferences()) {
            if (!refId.equals(id)) {
                ServiceInstance ref = instanceRepository.find(refId);
                Set<String> references = ref.getReferences()
                        .stream()
                        .filter((String i) -> ! i.equals(id))
                        .collect(Collectors.toSet());
                ref.setReferences(references);
                instanceRepository.save(ref);
            }
        }
    }

    @Override
    public ServiceInstance create(String bucketName, ServiceDefinitionProxy service, PlanProxy plan,
                                  Map<String, Object> parameters) {
        Map<String, Object> serviceSettings = ecs.createBucket(bucketName, service, plan, parameters);

        return getServiceInstance(serviceSettings);
    }
}
