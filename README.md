<pre>
 ____  __.       _____ __         ___________           .__   
|    |/ _|____ _/ ____\  | _______\__    ___/___   ____ |  |  
|      < \__  \\   __\|  |/ /\__  \ |    | /  _ \ /  _ \|  |  
|    |  \ / __ \|  |  |    <  / __ \|    |(  <_> |  <_> )  |__
|____|__ (____  /__|  |__|_ \(____  /____| \____/ \____/|____/
        \/    \/           \/     \/                          
                  _____                                       
                 /  _  \___  _________  ____                  
                /  /_\  \  \/ /\_  __ \/  _ \                 
               /    |    \   /  |  | \(  <_> )                
               \____|__  /\_/   |__|   \____/                 
                       \/                                     
</pre>

Kafka Tool Avro
================

Kafka Tool Avro plugin for [Kafka Tool](http://www.kafkatool.com/) provides a decorator for Avro messages that will show the actual contents of the Avro objects in a suitable format (JSON).

Installation
------------

* Download the [latest release](https://github.com/laymain/kafka-tool-avro/releases/latest) of the plugin.
* Copy the jar to the 'plugins' folder in the Kafka Tool installation folder.
* Restart Kafka Tool.

Usage
-----

* Navigate to the topic that you want to use the decorator with.
* In the "Content Types" drop-downs you should see the name "Avro".
* Select it and click on "Update"

After that, the messages/keys will be decorated using the Avro decorator.
The first time you will use the decorator on a Kafka connection, you will be asked for the schema registry endpoint.
(ex: http://schema-registry.mydomain.com:8081)
These endpoints are stored in a configuration file that you can edit through the menu _Tools > Avro plugin settings..._
