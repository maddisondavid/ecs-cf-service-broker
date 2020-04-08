package com.emc.ecs.cloudfoundry.broker.service;

import com.emc.ecs.cloudfoundry.broker.model.PlanProxy;
import com.emc.ecs.cloudfoundry.broker.model.ServiceDefinitionProxy;
import com.emc.ecs.cloudfoundry.broker.repository.ServiceInstance;
import com.emc.ecs.cloudfoundry.broker.repository.ServiceInstanceRepository;
import com.github.paulcwarren.ginkgo4j.Ginkgo4jRunner;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.springframework.cloud.servicebroker.exception.ServiceBrokerException;
import org.springframework.cloud.servicebroker.model.CreateServiceInstanceRequest;

import java.util.HashMap;
import java.util.Map;

import static com.emc.ecs.common.Fixtures.*;
import static com.github.paulcwarren.ginkgo4j.Ginkgo4jDSL.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.*;

@RunWith(Ginkgo4jRunner.class)
public class RemoteConnectionInstanceWorkflowTest {

    private EcsService ecs;
    private ServiceInstanceRepository instanceRepo;
    private Map<String, Object> params = new HashMap<>();
    private InstanceWorkflow workflow;
    private ServiceDefinitionProxy serviceProxy = new ServiceDefinitionProxy();
    private PlanProxy planProxy = new PlanProxy();
    private ArgumentCaptor<ServiceInstance> instCap = ArgumentCaptor.forClass(ServiceInstance.class);
    private ServiceInstance localInst;
    private Map<String, Object> settings;

    {
        Describe("RemoteConnectionInstanceWorkflow", () -> {
            BeforeEach(() -> {
                ecs = mock(EcsService.class);
                instanceRepo = mock(ServiceInstanceRepository.class);
                workflow = new RemoteConnectionInstanceWorkflow(instanceRepo, ecs);
            });

            Context("#changePlan", () ->
                    It("should throw an exception as this operation isn't supported", () -> {
                        try {
                            workflow.changePlan(SERVICE_INSTANCE_ID, serviceProxy, planProxy, params);
                        } catch (ServiceBrokerException e) {
                            assertEquals(e.getClass(), ServiceBrokerException.class);
                        }
                    })
            );

            Context("#delete", () ->
                    It("should throw an exception as this operation isn't supported", () -> {
                        try {
                            workflow.delete(SERVICE_INSTANCE_ID);
                        } catch (ServiceBrokerException e) {
                            assertEquals(e.getClass(), ServiceBrokerException.class);
                        }
                    })
            );

            Context("#create", () -> {

                BeforeEach(() -> {
                    params.put("remote_connection", remoteConnect(BUCKET_NAME, REMOTE_CONNECT_KEY));
                    CreateServiceInstanceRequest createReq = bucketCreateRequestFixture(params)
                            .withServiceInstanceId(SERVICE_INSTANCE_ID);
                    workflow.withCreateRequest(createReq);
                });

                Context("when remote instance doesn't exist", () -> {

                    BeforeEach(() ->
                            when(instanceRepo.find(BUCKET_NAME))
                                    .thenReturn(null));

                    It("should raise an exception", () -> {
                        try {
                            workflow.create(SERVICE_INSTANCE_ID, serviceProxy, planProxy, params);
                        } catch (ServiceBrokerException e) {
                            String message = "Remotely connected service instance not found";
                            assertEquals(ServiceBrokerException.class, e.getClass());
                            assertEquals(message, e.getMessage());
                        }
                    });


                });







                Context("with valid remote connect credentials", () -> {
                    BeforeEach(() -> {
                        settings = resolveSettings(serviceProxy, planProxy, new HashMap<>());
                        ServiceInstance remoteInst = new ServiceInstance(remoteBucketCreateRequestFixture(params));
                        remoteInst.addRemoteConnectionKey(BINDING_ID, REMOTE_CONNECT_KEY);
                        remoteInst.setServiceSettings(settings);
                        when(instanceRepo.find(BUCKET_NAME))
                                .thenReturn(remoteInst);
                    });

                    Context("when service definitions don't match", () ->
                            It("should raise an exception", () -> {
                                try {
                                    Map<String, Object> newParams = new HashMap<>();
                                    newParams.putAll(params);
                                    newParams.put("encrypted", true);
                                    workflow.create(SERVICE_INSTANCE_ID, serviceProxy, planProxy, newParams);
                                } catch (ServiceBrokerException e) {
                                    String message = "service definition must match between local and remote instances";
                                    assertEquals(ServiceBrokerException.class, e.getClass());
                                    assertEquals(message, e.getMessage());
                                }
                            })
                    );

                    Context("when service definitions match", () -> {
                        BeforeEach(() -> {
                            localInst = workflow.create(SERVICE_INSTANCE_ID, serviceProxy, planProxy, params);
                            instCap = ArgumentCaptor.forClass(ServiceInstance.class);
                            verify(instanceRepo, times(1))
                                    .save(instCap.capture());
                        });

                        It("should find the remote instance", () ->
                                verify(instanceRepo, times(1))
                                        .find(BUCKET_NAME));

                        It("should save the the remote service instance", () ->
                                assertEquals(REMOTE_SERVICE_INSTANCE_ID,
                                        instCap.getValue().getServiceInstanceId()));

                        It("should save the remote references", () -> {
                            ServiceInstance remoteInst = instCap.getValue();
                            assertEquals(2, remoteInst.getReferenceCount());
                            assert (remoteInst.getReferences().contains(REMOTE_SERVICE_INSTANCE_ID));
                            assert (remoteInst.getReferences().contains(SERVICE_INSTANCE_ID));
                        });

                        It("should save the remote service settings", () ->
                                assertEquals(settings,
                                        instCap.getValue().getServiceSettings()));

                        It("should return the local instance", () ->
                                assertEquals(SERVICE_INSTANCE_ID,
                                        localInst.getServiceInstanceId()));

                        It("should save the local references", () -> {
                            assertEquals(2, instCap.getValue().getReferenceCount());
                            assert (localInst.getReferences().contains(REMOTE_SERVICE_INSTANCE_ID));
                            assert (localInst.getReferences().contains(SERVICE_INSTANCE_ID));
                        });

                        It("should not yet have service settings", () ->
                                assertNull(localInst.getServiceSettings()));

                    });

                });

            });

        });

    }

    private Map<String, String> remoteConnect(String instanceId, String secretKey) {
        Map<String, String> remoteConnection = new HashMap<>();
        remoteConnection.put("accessKey", BINDING_ID);
        remoteConnection.put("secretKey", secretKey);
        remoteConnection.put("instanceId", instanceId);
        return remoteConnection;
    }

    private Map<String, Object> resolveSettings(
            ServiceDefinitionProxy service,
            PlanProxy plan,
            Map<String, Object> params
    ) {
        params.putAll(plan.getServiceSettings());
        params.putAll(service.getServiceSettings());
        return params;
    }

}
