package com.emc.ecs.servicebroker.config;

import com.emc.ecs.servicebroker.EcsManagementClientException;
import com.emc.ecs.servicebroker.EcsManagementResourceNotFoundException;
import com.emc.ecs.servicebroker.repository.ServiceInstanceBindingRepository;
import com.emc.ecs.servicebroker.repository.ServiceInstanceRepository;
import com.emc.ecs.servicebroker.service.EcsService;
import com.emc.ecs.servicebroker.service.EcsServiceInstanceBindingService;
import com.emc.ecs.servicebroker.service.EcsServiceInstanceService;
import com.emc.ecs.management.sdk.Connection;
import com.emc.ecs.utils.BucketWipe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.servicebroker.model.BrokerApiVersion;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;

import java.net.URISyntaxException;

@SuppressWarnings("unused")
@SpringBootApplication
public class Application {

    private static final Logger logger = LoggerFactory.getLogger(Application.class);
    private static ConfigurableApplicationContext context;

    private static String[] args;

    @Autowired
    private BrokerConfig broker;

    /* start application */
    public static void main(String[] args) {
        setArgs(args);
        Application.context = SpringApplication.run(Application.class, getArgs());
    }

    /* restart application */
    public static void main() {
        Application.context.close();
        Application.context = SpringApplication.run(Application.class, getArgs());
    }

    @Bean
    public Connection ecsConnection() {
	if (broker.getCertificate() != null) {
	    logger.info("Instantiating ecs connection with certificate");

        return new Connection(broker.getManagementEndpoint(),
                broker.getUsername(), broker.getPassword(), broker.getCertificate());
	} else {
        logger.info("Instantiating unencrypted ecs connection");
		return new Connection(broker.getManagementEndpoint(),
                broker.getUsername(), broker.getPassword());
	}
    }

    @Bean
    public BrokerApiVersion brokerApiVersion() {
        return new BrokerApiVersion(broker.getBrokerApiVersion());
    }

    @Bean
    public EcsService ecsService() {
        return new EcsService();
    }

    @Bean
    public EcsServiceInstanceBindingService ecsServiceInstanceBindingService()
            throws EcsManagementClientException,
            EcsManagementResourceNotFoundException, URISyntaxException {
        return new EcsServiceInstanceBindingService();
    }

    @Bean
    public ServiceInstanceRepository serviceInstanceRepository() {
        return new ServiceInstanceRepository();
    }

    @Bean
    public EcsServiceInstanceService ecsServiceInstanceService()
            throws EcsManagementClientException,
            EcsManagementResourceNotFoundException, URISyntaxException {
        return new EcsServiceInstanceService();
    }

    @Bean
    public ServiceInstanceBindingRepository serviceInstanceBindingRepository() {
        return new ServiceInstanceBindingRepository();
    }

    @Bean
    public BucketWipe ecsBucketWiper() {
        return new BucketWipe();
    }


    private static String[] getArgs() {
        return args;
    }

    private static void setArgs(String[] args) {
        Application.args = args;
    }

    public BrokerConfig getBroker() {
        return broker;
    }

    public void setBroker(BrokerConfig broker) {
        this.broker = broker;
    }
}
