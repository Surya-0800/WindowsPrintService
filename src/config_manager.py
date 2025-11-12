"""
Configuration Manager
Handles all service configuration with automatic defaults
"""

import json
import os
import uuid
import logging
from pathlib import Path
from typing import Dict, Any

class ConfigManager:
    """Manages service configuration with automatic setup"""
    
    def __init__(self, config_path: str = None):
        self.logger = logging.getLogger(__name__)
        
        # Determine configuration path
        if config_path:
            self.config_path = Path(config_path)
        else:
            app_data = os.environ.get('PROGRAMDATA', os.path.expanduser('~'))
            self.config_dir = Path(app_data) / "WindowsPrintService"
            self.config_path = self.config_dir / "config.json"
        
        self.config_dir = self.config_path.parent
        
        # Create all required directories
        self._create_directories()
        
        # Load or create configuration
        self._load_config()
    
    def _create_directories(self):
        """Create all required directories"""
        directories = [
            self.config_dir,
            self.config_dir / "logs",
            self.config_dir / "temp",
            self.config_dir / "cache"
        ]
        
        for directory in directories:
            try:
                directory.mkdir(parents=True, exist_ok=True)
                self.logger.debug(f"Directory ensured: {directory}")
            except Exception as e:
                self.logger.error(f"Failed to create directory {directory}: {e}")
    
    def _load_config(self):
        """Load configuration from file or create default"""
        try:
            if self.config_path.exists():
                with open(self.config_path, 'r', encoding='utf-8') as f:
                    self.config = json.load(f)
                self.logger.info("Configuration loaded successfully")
                
                # Ensure all required keys exist
                default_config = self._create_default_config()
                for key, value in default_config.items():
                    if key not in self.config:
                        self.config[key] = value
                        self.logger.info(f"Added missing config key: {key}")
                
            else:
                self.config = self._create_default_config()
                self._save_config()
                self.logger.info("Default configuration created")
                
        except Exception as e:
            self.logger.error(f"Configuration load error: {e}")
            self.config = self._create_default_config()
            self._save_config()
    
    def _create_default_config(self) -> Dict[str, Any]:
        """Create default configuration"""
        machine_name = os.environ.get('COMPUTERNAME', 'Unknown')
        client_id = f"{machine_name}_{str(uuid.uuid4())[:8]}"
        
        return {
            # Server connection
            "server_url": "http://localhost:8000",
            "client_id": client_id,
            "api_key": "",
            
            # Polling configuration (ultra-fast)
            "poll_interval": 0.1,  # 100ms
            "max_retries": 3,
            "timeout_seconds": 30,
            
            # Local API server
            "local_api_port": 8081,
            "enable_cors": True,
            
            # Directories
            "temp_directory": str(self.config_dir / "temp"),
            "log_directory": str(self.config_dir / "logs"),
            "cache_directory": str(self.config_dir / "cache"),
            
            # Logging
            "log_level": "INFO",
            "log_max_size": "10MB",
            "log_backup_count": 5,
            
            # Performance
            "max_concurrent_jobs": 5,
            "enable_caching": True,
            "cache_max_size": 100,
            
            # Features
            "enable_burst_mode": True,
            "burst_duration": 5,
            "burst_interval": 0.05,  # 50ms
            "auto_printer_refresh": True,
            "printer_refresh_interval": 300  # 5 minutes
        }
    
    def _save_config(self):
        """Save configuration to file"""
        try:
            # Create backup if config exists
            if self.config_path.exists():
                backup_path = self.config_path.with_suffix('.json.backup')
                self.config_path.replace(backup_path)
            
            # Save new configuration
            with open(self.config_path, 'w', encoding='utf-8') as f:
                json.dump(self.config, f, indent=2, ensure_ascii=False)
            
            self.logger.debug("Configuration saved successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to save configuration: {e}")
    
    def get_config(self) -> Dict[str, Any]:
        """Get current configuration (copy)"""
        return self.config.copy()
    
    def update_config(self, updates: Dict[str, Any]):
        """Update configuration with new values"""
        try:
            self.config.update(updates)
            self._save_config()
            self.logger.info(f"Configuration updated: {list(updates.keys())}")
        except Exception as e:
            self.logger.error(f"Failed to update configuration: {e}")
    
    def get_server_config(self) -> Dict[str, Any]:
        """Get server-related configuration"""
        return {
            "server_url": self.config["server_url"],
            "client_id": self.config["client_id"],
            "api_key": self.config["api_key"],
            "poll_interval": self.config["poll_interval"],
            "max_retries": self.config["max_retries"],
            "timeout_seconds": self.config["timeout_seconds"]
        }
    
    def get_api_config(self) -> Dict[str, Any]:
        """Get local API configuration"""
        return {
            "local_api_port": self.config["local_api_port"],
            "enable_cors": self.config["enable_cors"]
        }
    
    def get_performance_config(self) -> Dict[str, Any]:
        """Get performance-related configuration"""
        return {
            "max_concurrent_jobs": self.config["max_concurrent_jobs"],
            "enable_caching": self.config["enable_caching"],
            "cache_max_size": self.config["cache_max_size"],
            "enable_burst_mode": self.config["enable_burst_mode"],
            "burst_duration": self.config["burst_duration"],
            "burst_interval": self.config["burst_interval"]
        }