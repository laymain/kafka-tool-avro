package com.laymain.kafkatool.plugin;

import com.kafkatool.external.ICustomMessageDecorator;
import com.kafkatool.ui.MainFrame;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.swing.*;
import java.awt.*;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class AvroCustomMessageDecorator implements ICustomMessageDecorator {

    private static final Logger LOGGER = LoggerFactory.getLogger(AvroCustomMessageDecorator.class);
    private static final String DISPLAY_NAME = "Avro";
    private static final String PROPERTIES_FILE = String.join(File.separator, System.getProperty("user.home"), ".com.laymain.kafkatool.plugin.avro.properties");
    private static final Properties SCHEMA_REGISTY_ENDPOINTS = loadProperties();

    private final ConcurrentMap<String, KafkaAvroDeserializer> deserializers = new ConcurrentHashMap<>();
    private final AtomicBoolean configurationDialogOpened = new AtomicBoolean(false);
    private final AtomicBoolean menuBarInjectionDone = new AtomicBoolean(false);

    @Override
    public String getDisplayName() {
        return DISPLAY_NAME;
    }

    @Override
    public String decorate(String zookeeperHost, String brokerHost, String topic, long partitionId, long offset, byte[] bytes, Map<String, String> map) {
        injectMenuItem();
        String schemaRegistryEndpoint = getSchemaRegistryEndpoint(zookeeperHost);
        if (schemaRegistryEndpoint == null || schemaRegistryEndpoint.isEmpty()) {
            return "Missing schema registry endpoint";
        }
        try {
            deserializers.computeIfAbsent(schemaRegistryEndpoint, key -> new KafkaAvroDeserializer(new CachedSchemaRegistryClient(key, 10)));
            KafkaAvroDeserializer deserializer = deserializers.get(schemaRegistryEndpoint);
            Object deserializedObject = deserializer.deserialize(topic, bytes);
            if (deserializedObject instanceof String) {
                return deserializedObject;
            }
            return ((GenericRecord)deserializedObject).toString();            
        } catch (Exception e) {
            LOGGER.error("Cannot decorate message", e);
            return String.format("Error: %s", e);
        }
    }

    private String getSchemaRegistryEndpoint(String zookeeperHost) {
        if (SCHEMA_REGISTY_ENDPOINTS.containsKey(zookeeperHost)) {
            return SCHEMA_REGISTY_ENDPOINTS.getProperty(zookeeperHost);
        }
        if (configurationDialogOpened.compareAndSet(false, true)) {
            // Double check
            if (SCHEMA_REGISTY_ENDPOINTS.containsKey(zookeeperHost)) {
                configurationDialogOpened.set(false);
                return SCHEMA_REGISTY_ENDPOINTS.getProperty(zookeeperHost);
            }
            SwingUtilities.invokeLater(() -> {
                if (!SCHEMA_REGISTY_ENDPOINTS.containsKey(zookeeperHost)) {
                    String endpoint = JOptionPane.showInputDialog(String.format("Enter schema registry endpoint for %s", zookeeperHost));
                    if (endpoint != null && !endpoint.isEmpty()) {
                        SCHEMA_REGISTY_ENDPOINTS.setProperty(zookeeperHost, endpoint);
                        saveProperties(SCHEMA_REGISTY_ENDPOINTS);
                    }
                }
                configurationDialogOpened.set(false);
            });
        }
        return null;
    }

    private static Properties loadProperties() {
        Properties properties = new Properties();
        try {
            File propertiesFile = Paths.get(PROPERTIES_FILE).toFile();
            if (propertiesFile.exists()) {
                try (FileInputStream input = new FileInputStream(PROPERTIES_FILE)) {
                    properties.load(input);
                }
            } else {
                //noinspection ResultOfMethodCallIgnored
                propertiesFile.createNewFile(); //NOSONAR
            }
        } catch (Exception e) {
            LOGGER.error("Cannot load properties", e.toString());
            JOptionPane.showMessageDialog(MainFrame.getInstance(), e.toString(), "Cannot load avro plugin properties", JOptionPane.ERROR_MESSAGE);
        }
        return properties;
    }

    private static void saveProperties(Properties properties) {
        try (FileOutputStream output = new FileOutputStream(PROPERTIES_FILE)) {
            properties.store(output, "Schema registry per cluster endpoints");
        } catch (Exception e) {
            LOGGER.error("Cannot save properties", e.toString());
            JOptionPane.showMessageDialog(MainFrame.getInstance(), e.toString(), "Cannot save avro plugin properties", JOptionPane.ERROR_MESSAGE);
        }
    }

    private static final int MENUBAR_TOOLS_INDEX = 2;
    private static final String PLUGIN_MENU_ITEM_CAPTION = "Avro plugin settings...";
    private void injectMenuItem() {
        if (menuBarInjectionDone.compareAndSet(false, true)) {
            JMenu menu = MainFrame.getInstance().getJMenuBar().getMenu(MENUBAR_TOOLS_INDEX);
            if (menu != null) {
                menu.addSeparator();
                JMenuItem menuItem = new JMenuItem(PLUGIN_MENU_ITEM_CAPTION, PLUGIN_MENU_ITEM_CAPTION.charAt(0));
                menuItem.addActionListener(actionEvent -> editProperties(Paths.get(PROPERTIES_FILE).toFile()));
                menu.add(menuItem);
            }
        }
    }

    private static void editProperties(File propertyFile) {
        try {
            Desktop.getDesktop().edit(propertyFile);
        } catch (IOException e0) {
            LOGGER.error("Cannot edit configuration file", e0);
            try {
                Desktop.getDesktop().open(propertyFile);
            } catch (IOException e1) {
                final String message = "Cannot open configuration file in editor";
                LOGGER.error(message, e1);
                JOptionPane.showMessageDialog(MainFrame.getInstance(), e1.toString(), message, JOptionPane.ERROR_MESSAGE);
            }
        }
    }
}
