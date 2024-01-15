package dev.garvis.somcroguecraft;

import dev.garvis.somcroguecraft.Events;

import org.bukkit.plugin.java.JavaPlugin;

public class SoMCRoguecraftPlugin extends JavaPlugin {
    
    @Override
    public void onEnable() {
	getServer().getPluginManager().registerEvents(new Events(this), this);
    }

    @Override
    public void onDisable() {
    }
}
