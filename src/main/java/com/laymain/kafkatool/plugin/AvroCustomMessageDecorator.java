package com.laymain.kafkatool.plugin;

import com.kafkatool.external.ICustomMessageDecorator;
import com.kafkatool.ui.MainFrame;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.swing.*;
import java.awt.*;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.net.URLDecoder;
import java.nio.charset.Charset;
import java.nio.file.Paths;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class AvroCustomMessageDecorator implements ICustomMessageDecorator {

    private static final Logger LOGGER = LoggerFactory.getLogger(AvroCustomMessageDecorator.class);
    private static final String DISPLAY_NAME = "Avro";
    private static final String PROPERTIES_FILE = String.join(File.separator, System.getProperty("user.home"), ".com.laymain.kafkatool.plugin.avro.properties");
    private static final Properties SCHEMA_REGISTY_ENDPOINTS = loadProperties();
    private static final String CONFIGURATION_PROPERTIS_DELIMITER = ".";
    
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
        	Map<String, String> config = getSchemaRegistryClientConfig(zookeeperHost);
        	
        	LOGGER.info("Schem registry client config: " + config.toString());
        	
            deserializers.computeIfAbsent(schemaRegistryEndpoint, key -> new KafkaAvroDeserializer(new CachedSchemaRegistryClient(key, 10, config)));
            KafkaAvroDeserializer deserializer = deserializers.get(schemaRegistryEndpoint);
            return String.valueOf(deserializer.deserialize(topic, bytes));
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
                    String endpointInput = JOptionPane.showInputDialog(String.format("Enter schema registry endpoint for %s", zookeeperHost));
                    if (endpointInput != null && !endpointInput.isEmpty()) {
                    	
                    	try {
                    		String endpoint = null;
                    		Map<String, String> config = null;
                    		int queryDelimiterIndex = endpointInput.indexOf("?");
                    		if(queryDelimiterIndex != -1) {
                    			endpoint = endpointInput.substring(0, queryDelimiterIndex);
                    			String query = endpointInput.substring(queryDelimiterIndex+1);
                    			config = Arrays.stream(query.split("&"))
                    				.map(param -> param.split("="))
                    				.filter(paramTokens -> paramTokens.length > 1)
                    				.map(paramTokens -> {
                    					try {
                    						return new String[]{URLDecoder.decode(paramTokens[0], Charset.defaultCharset().toString()),
                    								URLDecoder.decode(paramTokens[1],Charset.defaultCharset().toString())
                    							};
                    					} catch(Exception e) {
                    						throw new RuntimeException(e);
                    					}
                    				})
                    				.collect(Collectors.toMap(paramTokens -> paramTokens[0], paramTokens -> paramTokens[1]));
                    		} else {
                    			endpoint = endpointInput;
                    		}
                    		
                    		LOGGER.info(String.format("Endpoint: %s, config: %s", endpoint, config));
                    		
                            SCHEMA_REGISTY_ENDPOINTS.setProperty(zookeeperHost, endpoint);
                            if(Objects.nonNull(config)) {
                            	config.forEach((key, value) -> SCHEMA_REGISTY_ENDPOINTS.setProperty(zookeeperHost + CONFIGURATION_PROPERTIS_DELIMITER + key , value));
                            }
                            saveProperties(SCHEMA_REGISTY_ENDPOINTS);                    		
                    	} catch(Exception e) {
                            LOGGER.error("Cannot process URL", e.toString());
                            JOptionPane.showMessageDialog(MainFrame.getInstance(), e.toString(), "Cannot process URL", JOptionPane.ERROR_MESSAGE);
                    	}
                    }
                }
                configurationDialogOpened.set(false);
            });
        }
        return null;
    }
    
    protected Map<String, String> getSchemaRegistryClientConfig(String zookeeperHost) {
    	return SCHEMA_REGISTY_ENDPOINTS.stringPropertyNames().stream()
    		.filter(propertyName -> propertyName.startsWith(zookeeperHost + CONFIGURATION_PROPERTIS_DELIMITER))
    		.map(propertyName ->  {
    			 return new AbstractMap.SimpleEntry<String, String>(
    					 propertyName.substring((zookeeperHost + CONFIGURATION_PROPERTIS_DELIMITER).length()), 
    					 SCHEMA_REGISTY_ENDPOINTS.getProperty(propertyName));
    		}).collect(Collectors.toMap(entry -> entry.getKey(), entry -> entry.getValue()));
    		
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
