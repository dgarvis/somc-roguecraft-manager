package dev.garvis.somcroguecraft;

import dev.garvis.somcroguecraft.KafkaManager;
import dev.garvis.somcroguecraft.Events;

import org.bukkit.Bukkit;
import org.bukkit.plugin.java.JavaPlugin;
import org.bukkit.configuration.file.FileConfiguration;
import org.bukkit.ChatColor;

import java.util.List;
import java.util.LinkedList;
import java.util.Map;
import java.util.Arrays;

public class SoMCRoguecraftPlugin extends JavaPlugin {
    
    private KafkaManager kafka = new KafkaManager();
    private boolean running = true;

    @Override
    public void onEnable() {
	this.saveDefaultConfig();
	this.attemptToConnectToKafka();

	getServer().getPluginManager().registerEvents(new Events(this), this);
    }

    private void updateWorldState(String state) {
	String world = getConfig().getString("bungeeWorldName");
	
	KafkaManager.Message e = this.kafka.new Message();
	e.put("eventType", "WORLD_STATE_UPDATE");
	e.put("world", world);
	e.put("state", state);
	kafka.sendMessage(e);	
    }

    private void attemptToConnectToKafka() {
	if (!this.running) return;

	final String[] events = {
	    "WORLD_JOIN_APPROVED"
	};

	reloadConfig();
	FileConfiguration config = getConfig();
	String kafkaServer = config.getString("kafkaServer");
	String kafkaTopic = config.getString("kafkaTopic");
	String kafkaName = config.getString("kafkaName");

	// Check we have a kafka server configured
	if (kafkaServer.isEmpty()) {
	    getLogger().warning("Kafka connection not configured.");
	    Bukkit.getScheduler().runTaskLaterAsynchronously(this, () -> {
		    attemptToConnectToKafka();
		}, 20 * 60); // wait 60 second and try again. 20 ticks / second.
	    return;
	}

	// Try to connect to kafka
	try {
	    kafka.connect(kafkaName, kafkaServer, kafkaTopic,
			  new String[]{kafkaTopic}, events,
			  (LinkedList<KafkaManager.Message> messages) -> {
			      this.processMessages(messages);
			  });
	    getLogger().info("Connected to Kafka");
	} catch (Exception e) {
	    getLogger().warning("Not connected to kafka, check plugin config.");
	    Bukkit.getScheduler().runTaskLaterAsynchronously(this, () -> {
		    attemptToConnectToKafka();
		}, 20 * 60); // wait 60 second and try again. 20 ticks / second.
	}
    }

    private void processMessages(LinkedList<KafkaManager.Message> messages) {

	Bukkit.getScheduler().runTask(this, () -> {
		for (KafkaManager.Message message : messages) {
		    System.out.println("Got Message: " + message.toString());

		    switch ((String)message.get("eventType")) {
		    case "WORLD_JOIN_APPROVED":

			String world = getConfig().getString("bungeeWorldName");

			// Are they asking to be transfered to our world?
			if (!((String)message.get("world")).equals(world)) return;

			// Tranfer them to our world.
			KafkaManager.Message e = this.kafka.new Message();
			e.put("eventType", "TRANSFER_PLAYER");
			e.put("playerName", (String)message.get("playerName"));
			e.put("world", world);
			kafka.sendMessage(e);
			break;
		    }
		}
	    });
    }

    @Override
    public void onDisable() {
	this.running = false;
	kafka.close();
	getLogger().info("Spigot Events Disabled");
    }
}
