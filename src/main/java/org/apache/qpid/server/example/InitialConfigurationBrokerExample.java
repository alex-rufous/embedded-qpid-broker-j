/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package org.apache.qpid.server.example;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.function.Consumer;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.SystemLauncher;
import org.apache.qpid.server.SystemLauncherListener;
import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.model.Container;
import org.apache.qpid.server.model.Exchange;
import org.apache.qpid.server.model.Port;
import org.apache.qpid.server.model.Queue;
import org.apache.qpid.server.model.SystemConfig;
import org.apache.qpid.server.model.VirtualHost;
import org.apache.qpid.server.model.VirtualHostNode;
import org.apache.qpid.server.model.port.AmqpPort;

public class InitialConfigurationBrokerExample
{
    private static final Logger LOGGER = LoggerFactory.getLogger(InitialConfigurationBrokerExample.class);

    private static final String QPID_WORK_DIR_PREFIX = "qpid-work";
    private static final String PREFERENCES_NOOP = "{\"type\": \"Noop\"}";
    private static final URL INITIAL_CONFIGURATION =
            InitialConfigurationBrokerExample.class.getClassLoader().getResource("test-initial-config.json");
    private static final String DEFAULT_DIRECT_EXCHANGE = "amq.direct";
    private static final String USERNAME = "guest";
    private static final String PASSWORD = "guest";
    private static final String QUEUE_NAME = "test";

    public static void main(String[] args) throws Exception
    {
        final InitialConfigurationBrokerExample example = new InitialConfigurationBrokerExample();
        example.startBroker(example::createQueueAndSendAndReceiveMessage);
    }

    private void startBroker(Consumer<Container<?>> brokerOperation) throws IOException
    {
        final Path workingDirectory = Files.createTempDirectory(QPID_WORK_DIR_PREFIX);
        final File workingDirectoryFile = workingDirectory.toFile();
        try
        {
            final Container<?> container = createBrokerContainer(workingDirectoryFile);
            try
            {
                brokerOperation.accept(container);
            }
            finally
            {
                container.getParent().close();
            }
        }
        finally
        {
            Files.walk(workingDirectory)
                 .sorted(Comparator.reverseOrder())
                 .map(Path::toFile)
                 .forEach(f -> {
                     if (!f.delete())
                     {
                         throw new IllegalConfigurationException(String.format(
                                 "Cannot clean-up working directory %s", workingDirectory.toFile().getAbsolutePath()));
                     }
                 });
        }
    }

    private void createQueueAndSendAndReceiveMessage(final Container<?> container)
    {
        createAndBindTestQueue(container);
        sendAndReceive(container);
    }

    private void sendAndReceive(final Container<?> container)
    {
        final int boundPort = container.getChildren(Port.class)
                                       .stream()
                                       .filter(p -> p instanceof AmqpPort)
                                       .findFirst()
                                       .orElseThrow(() -> new IllegalConfigurationException(
                                               "Cannot find amqp port"))
                                       .getBoundPort();

        LOGGER.info("Found amqp port {}", boundPort);

        final VirtualHost<?> virtualHost = getVirtualHost(container);
        try
        {
            ConnectionFactory factory = getConnectionFactory(boundPort, virtualHost.getName());
            sendAndReceive(factory);
        }
        catch (NamingException | JMSException e)
        {
            LOGGER.error("Cannot send and receive message", e);
        }
    }

    private void createAndBindTestQueue(final Container<?> container)
    {
        final VirtualHost<?> virtualHost = getVirtualHost(container);
        final Queue<?> queue = virtualHost.createChild(Queue.class, Collections.singletonMap(Queue.NAME, QUEUE_NAME));

        LOGGER.info("Queue '{}' is created", QUEUE_NAME);

        @SuppressWarnings("unchecked")
        final Exchange<?> exchange = (Exchange<?>) virtualHost.findConfiguredObject(Exchange.class, DEFAULT_DIRECT_EXCHANGE);

        exchange.bind(queue.getName(), queue.getName(), null, false);

        LOGGER.info("Queue '{}' is bound to exchange '{}'", QUEUE_NAME, DEFAULT_DIRECT_EXCHANGE);
    }

    private VirtualHost<?> getVirtualHost(final Container<?> container)
    {
        final VirtualHost<?> virtualHost = container.getChildren(VirtualHostNode.class)
                                                    .stream()
                                                    .findFirst()
                                                    .orElseThrow(() -> new IllegalConfigurationException(
                                                            "Cannot find virtual host node"))
                                                    .getVirtualHost();
        if (virtualHost == null)
        {
            throw new IllegalConfigurationException("Cannot find virtual host");
        }
        return virtualHost;
    }

    private Container<?> createBrokerContainer(final File workDir)
    {
        assert INITIAL_CONFIGURATION != null;

        final Map<String, String> context = new HashMap<>();
        context.put("qpid.work_dir", workDir.getAbsolutePath());
        context.put("qpid.amqp_port", "0");
        context.put("qpid.broker.defaultPreferenceStoreAttributes", PREFERENCES_NOOP);
        context.put("qpid.user.name", USERNAME);
        context.put("qpid.user.password", PASSWORD);

        final Map<String, Object> attributes = new HashMap<>();
        attributes.put("type", "JSON");
        attributes.put("initialConfigurationLocation", INITIAL_CONFIGURATION.toExternalForm());
        attributes.put("context", context);
        attributes.put("storePath", "${json:qpid.work_dir}${file.separator}config.json");
        attributes.put("startupLoggedToSystemOut", "false");

        final ContainerDiscoverer containerDiscoverer = new ContainerDiscoverer();
        final SystemLauncher systemLauncher = new SystemLauncher(containerDiscoverer);
        try
        {
            systemLauncher.startup(attributes);
        }
        catch (Exception e)
        {
            throw new IllegalConfigurationException("Unexpected startup failure", e);
        }
        return containerDiscoverer.getContainer();
    }

    private ConnectionFactory getConnectionFactory(int port, final String virtualhost) throws NamingException
    {
        Properties properties = new Properties();
        properties.put("java.naming.factory.initial", "org.apache.qpid.jms.jndi.JmsInitialContextFactory");
        properties.put("connectionfactory.qpidConnectionFactory", String.format("amqp://localhost:%d?amqp.vhost=%s", port, virtualhost));
        Context context = new InitialContext(properties);
        try
        {
            return (ConnectionFactory) context.lookup("qpidConnectionFactory");
        }
        finally
        {
            context.close();
        }
    }

    private void sendAndReceive(final ConnectionFactory connectionFactory) throws JMSException
    {
        Connection connection = connectionFactory.createConnection(InitialConfigurationBrokerExample.USERNAME,
                                                                   InitialConfigurationBrokerExample.PASSWORD);
        try
        {
            connection.start();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Destination destination = session.createQueue(InitialConfigurationBrokerExample.QUEUE_NAME);

            MessageProducer messageProducer = session.createProducer(destination);
            MessageConsumer messageConsumer = session.createConsumer(destination);
            TextMessage message = session.createTextMessage("Hello world!");
            messageProducer.send(message);

            TextMessage receivedMessage = (TextMessage) messageConsumer.receive();
            LOGGER.info("Message received:" + receivedMessage.getText());
        }
        finally
        {

            connection.close();
        }
    }

    private static class ContainerDiscoverer extends SystemLauncherListener.DefaultSystemLauncherListener
    {
        private SystemConfig<?> _systemConfig;

        @Override
        public void onContainerResolve(final SystemConfig<?> systemConfig)
        {
            super.onContainerResolve(systemConfig);
            _systemConfig = systemConfig;
        }

        Container<?> getContainer()
        {
            if (_systemConfig == null)
            {
                throw new IllegalConfigurationException("Container was not resolved");
            }
            return _systemConfig.getContainer();
        }
    }
}
