package com.emc.ecs.cloudfoundry.broker.service;

import com.emc.ecs.cloudfoundry.broker.EcsManagementClientException;
import com.emc.ecs.cloudfoundry.broker.EcsManagementResourceNotFoundException;
import com.emc.ecs.cloudfoundry.broker.model.PlanProxy;
import com.emc.ecs.cloudfoundry.broker.model.ServiceDefinitionProxy;
import com.emc.ecs.cloudfoundry.broker.repository.ServiceInstance;
import com.emc.ecs.cloudfoundry.broker.repository.ServiceInstanceRepository;
import org.springframework.cloud.servicebroker.exception.ServiceBrokerException;

import javax.xml.bind.JAXBException;
import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class NamespaceInstanceWorkflow extends InstanceWorkflowImpl {
    NamespaceInstanceWorkflow(ServiceInstanceRepository instanceRepo, EcsService ecs) {
        super(instanceRepo, ecs);
    }

    @Override
    public Map<String, Object> changePlan(String instanceName, ServiceDefinitionProxy service, PlanProxy plan, Map<String, Object> parameters) throws EcsManagementClientException, IOException {
        return ecs.changeNamespacePlan(instanceName, service, plan, parameters);
    }

    @Override
    public void delete(String id) {
        try {
            ServiceInstance instance = instanceRepository.find(id);
            if (instance.getReferences().size() > 1) {
                removeInstanceFromReferences(instance, id);
            } else {
                ecs.deleteNamespace(instance.getName());
            }
        } catch (EcsManagementClientException | JAXBException | IOException e) {
            throw new ServiceBrokerException(e);
        }
    }

    private void removeInstanceFromReferences(ServiceInstance instance, String id) throws IOException, JAXBException {
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
    public ServiceInstance create(String id, ServiceDefinitionProxy service, PlanProxy plan, Map<String, Object> parameters)
            throws EcsManagementClientException, EcsManagementResourceNotFoundException {
        ServiceInstance instance = getServiceInstance(parameters);
        Map<String, Object> serviceSettings = ecs.createNamespace(instance.getName(), service, plan, parameters);

        instance.setServiceSettings(serviceSettings);

        return instance;
    }
}
