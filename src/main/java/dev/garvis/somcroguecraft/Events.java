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

	// If game is not currently in progress, but a player was switched to survival,
	// the game is now in progress.
	if (!gameInProgress && event.getNewGameMode() == GameMode.SURVIVAL) {
	    Bukkit.getLogger().info("Game Started");
	    gameInProgress = true;
	    return;
	}

	// If a game is in progress, but a player got switched to adventure mode,
	// the game is no longer in progress.
	if (gameInProgress && event.getNewGameMode() == GameMode.ADVENTURE) {
	    Bukkit.getLogger().info("Game Ended");
	    gameInProgress = false; // might need to check all players.
	    return;
	}
    }

    @EventHandler
    public void onPlayerJoin(PlayerJoinEvent event) {
	GameMode mode = event.getPlayer().getGameMode();

	// If the game is not in progress, but they are not in adventure mode,
	// we need to reset them... ie defeat them. It is possible they were in a
	// run that happened in the past and left early.
	if (!gameInProgress &&  mode != GameMode.ADVENTURE) {
	    event.getPlayer().getInventory().clear();
	    Bukkit.getServer().dispatchCommand(Bukkit.getConsoleSender(), "function roguecraft:run_end");
	    return;
	}


	// If a game is in progress, we need to check if the current player has the boss bar.
	// If they don't have it, they should be set to spectaor mode as they are not part of the
	// current run.
	if (gameInProgress) {

	    // We need to wait for the player to login and boss bars to be added to them for
	    // this next check.
	    Bukkit.getScheduler().runTaskLater(plugin, () -> {

		    boolean hasBar = false;
		    for (Player p : Bukkit
			     .getBossBar(NamespacedKey.fromString("minecraft:difficulty"))
			     .getPlayers()) {
			hasBar = hasBar || (p == event.getPlayer());
		    }
		
		    if (!hasBar) {
			event.getPlayer().setGameMode(GameMode.SPECTATOR);
			return;
		    }
		}, 20 * 3);
	}
    }
    
}
