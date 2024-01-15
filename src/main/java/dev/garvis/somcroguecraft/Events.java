package dev.garvis.somcroguecraft;

import org.bukkit.Bukkit;
import org.bukkit.plugin.java.JavaPlugin;
import org.bukkit.NamespacedKey;
import org.bukkit.entity.Player;
import org.bukkit.GameMode;
import org.bukkit.event.Listener;
import org.bukkit.event.EventHandler;
import org.bukkit.event.player.PlayerGameModeChangeEvent;
//import org.bukkit.event.player.PlayerLoginEvent;
import org.bukkit.event.player.PlayerJoinEvent;

public class Events implements Listener {

    private boolean gameInProgress = false;
    private JavaPlugin plugin;

    public Events(JavaPlugin plugin) {
	this.plugin = plugin;
    }
    
    @EventHandler
    public void onPlayerGameModeChange(PlayerGameModeChangeEvent event) {
	// If the players all changed to survial/spectator, than the game has started.
	// If a player changed from survival to spectaor than they died.
	// If a player changed from spectator to adventure than the game is done.

	if (!gameInProgress && event.getNewGameMode() == GameMode.SURVIVAL) {
	    Bukkit.getLogger().info("Game Started");
	    gameInProgress = true;
	    return;
	}

	if (gameInProgress && event.getNewGameMode() == GameMode.ADVENTURE) {
	    Bukkit.getLogger().info("Game Ended");
	    gameInProgress = false; // might need to check all players.
	    return;
	}
    }

    @EventHandler
    //public void onPlayerLogin(PlayerLoginEvent event) {
    public void onPlayerJoin(PlayerJoinEvent event) {
	GameMode mode = event.getPlayer().getGameMode();
	
	// If they are not in adventure mode and the game is not in session
	// call the function roguecraft:run_end
	if (!gameInProgress &&  mode != GameMode.ADVENTURE) {
	    event.getPlayer().getInventory().clear();
	    Bukkit.getServer().dispatchCommand(Bukkit.getConsoleSender(), "function roguecraft:run_end");
	    return;
	}


	// Does the player have the difficultly boss bar.
	Bukkit.getScheduler().runTaskLater(plugin, () -> {
		boolean hasBar = false;
		for (Player p : Bukkit.getBossBar(NamespacedKey.fromString("minecraft:difficulty")).getPlayers()) {
		    hasBar = hasBar || (p == event.getPlayer());
		    Bukkit.getLogger().info(p.getName());
		}
		Bukkit.getLogger().info(hasBar ? "true" : "false");
		
		// If a game is in session, and the player is at spawn, set them to specator mode.
		if (gameInProgress && !hasBar) {
		    // or they are more than 50,000 blocks from spawn? But what if they are in the nether...
		    // lets check if the player is part of the boss bar list for difficulty.
		    event.getPlayer().setGameMode(GameMode.SPECTATOR);
		    Bukkit.getLogger().info(event.getPlayer().getName() + " now spectator");
		    return;
		}
	    }, 20 * 3);
    }
    
}
