"""
Service Installer
Handles Windows service installation, management, and configuration
"""

import os
import sys
import subprocess
import logging
import time
from pathlib import Path
from typing import Optional

class ServiceInstaller:
    """Manages Windows service installation and lifecycle"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.service_name = "WindowsPrintService"
        self.service_display_name = "Windows Print Service - Ultra Fast"
        self.service_description = "Ultra-fast print service for web applications with zero-dialog printing"
    
    def install_service(self, server_url: str) -> bool:
        """Install the Windows service"""
        try:
            self.logger.info("Installing Windows Print Service...")
            
            # Get current executable path
            if getattr(sys, 'frozen', False):
                # Running as PyInstaller executable
                exe_path = sys.executable
            else:
                # Running as Python script
                exe_path = os.path.abspath(sys.argv[0])
            
            self.logger.info(f"Service executable: {exe_path}")
            
            # Create service using sc.exe
            cmd = [
                "sc.exe", "create", self.service_name,
                f"binPath= \"{exe_path}\" --service",
                f"DisplayName= {self.service_display_name}",
                "start= auto",
                "type= own"
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True)
            
            if result.returncode == 0:
                self.logger.info("✓ Service created successfully")
                
                # Set service description
                self._set_service_description()
                
                # Configure service to restart on failure
                self._configure_service_recovery()
                
                # Update configuration with server URL
                self._update_server_config(server_url)
                
                self.logger.info("✓ Service installation completed")
                
                # Start the service
                if self.start_service():
                    self.logger.info("✓ Service started successfully")
                    print("\n🎉 Installation Complete!")
                    print(f"Service Name: {self.service_name}")
                    print(f"Status: Running")
                    print(f"Local API: http://localhost:8081")
                    print(f"Django Server: {server_url}")
                    print("\nUse 'python main.py --status' to check service status")
                    return True
                else:
                    self.logger.warning("Service installed but failed to start")
                    return False
                    
            else:
                error_msg = result.stderr.strip()
                if "already exists" in error_msg.lower():
                    self.logger.warning("Service already exists. Use --uninstall first if you want to reinstall.")
                    return False
                else:
                    self.logger.error(f"Service creation failed: {error_msg}")
                    return False
                    
        except Exception as e:
            self.logger.error(f"Service installation failed: {e}")
            return False
    
    def _set_service_description(self):
        """Set the service description"""
        try:
            cmd = ["sc.exe", "description", self.service_name, self.service_description]
            subprocess.run(cmd, capture_output=True)
            self.logger.debug("Service description set")
        except Exception as e:
            self.logger.warning(f"Failed to set service description: {e}")
    
    def _configure_service_recovery(self):
        """Configure service to restart on failure"""
        try:
            cmd = [
                "sc.exe", "failure", self.service_name,
                "reset= 86400",  # Reset failure count after 24 hours
                "actions= restart/5000/restart/10000/restart/30000"  # Restart after 5s, 10s, 30s
            ]
            subprocess.run(cmd, capture_output=True)
            self.logger.debug("Service recovery configured")
        except Exception as e:
            self.logger.warning(f"Failed to configure service recovery: {e}")
    
    def _update_server_config(self, server_url: str):
        """Update configuration with server URL"""
        try:
            from config_manager import ConfigManager
            
            config_manager = ConfigManager()
            config_manager.update_config({"server_url": server_url})
            self.logger.info(f"Updated server URL to: {server_url}")
            
        except Exception as e:
            self.logger.warning(f"Failed to update server configuration: {e}")
    
    def uninstall_service(self) -> bool:
        """Uninstall the Windows service"""
        try:
            self.logger.info("Uninstalling Windows Print Service...")
            
            # Stop service first
            self.stop_service()
            
            # Delete service
            cmd = ["sc.exe", "delete", self.service_name]
            result = subprocess.run(cmd, capture_output=True, text=True)
            
            if result.returncode == 0:
                self.logger.info("✓ Service uninstalled successfully")
                print("\n✓ Service uninstalled successfully")
                return True
            else:
                error_msg = result.stderr.strip()
                if "does not exist" in error_msg.lower():
                    self.logger.info("Service was not installed")
                    print("Service was not installed")
                    return True
                else:
                    self.logger.error(f"Service uninstallation failed: {error_msg}")
                    return False
                    
        except Exception as e:
            self.logger.error(f"Service uninstallation failed: {e}")
            return False
    
    def start_service(self) -> bool:
        """Start the Windows service"""
        try:
            self.logger.info("Starting Windows Print Service...")
            
            cmd = ["sc.exe", "start", self.service_name]
            result = subprocess.run(cmd, capture_output=True, text=True)
            
            if result.returncode == 0:
                self.logger.info("✓ Service start command issued")
                
                # Wait for service to actually start
                if self._wait_for_service_status("RUNNING", timeout=30):
                    self.logger.info("✓ Service started successfully")
                    return True
                else:
                    self.logger.error("Service failed to start within timeout")
                    return False
                    
            else:
                error_msg = result.stderr.strip()
                if "already running" in error_msg.lower():
                    self.logger.info("Service is already running")
                    return True
                else:
                    self.logger.error(f"Service start failed: {error_msg}")
                    return False
                    
        except Exception as e:
            self.logger.error(f"Service start failed: {e}")
            return False
    
    def stop_service(self) -> bool:
        """Stop the Windows service"""
        try:
            self.logger.info("Stopping Windows Print Service...")
            
            cmd = ["sc.exe", "stop", self.service_name]
            result = subprocess.run(cmd, capture_output=True, text=True)
            
            if result.returncode == 0:
                self.logger.info("✓ Service stop command issued")
                
                # Wait for service to actually stop
                if self._wait_for_service_status("STOPPED", timeout=15):
                    self.logger.info("✓ Service stopped successfully")
                    return True
                else:
                    self.logger.warning("Service may not have stopped cleanly")
                    return True  # Consider it successful anyway
                    
            else:
                error_msg = result.stderr.strip()
                if "not started" in error_msg.lower() or "does not exist" in error_msg.lower():
                    self.logger.info("Service was not running")
                    return True
                else:
                    self.logger.error(f"Service stop failed: {error_msg}")
                    return False
                    
        except Exception as e:
            self.logger.error(f"Service stop failed: {e}")
            return False
    
    def _wait_for_service_status(self, target_status: str, timeout: int = 30) -> bool:
        """Wait for service to reach target status"""
        try:
            start_time = time.time()
            
            while time.time() - start_time < timeout:
                status = self._get_service_status()
                if status and target_status.upper() in status.upper():
                    return True
                
                time.sleep(1)
            
            return False
            
        except Exception as e:
            self.logger.error(f"Error waiting for service status: {e}")
            return False
    
    def _get_service_status(self) -> Optional[str]:
        """Get current service status"""
        try:
            cmd = ["sc.exe", "query", self.service_name]
            result = subprocess.run(cmd, capture_output=True, text=True)
            
            if result.returncode == 0:
                # Parse status from output
                for line in result.stdout.split('\n'):
                    if "STATE" in line:
                        parts = line.split()
                        if len(parts) >= 4:
                            return parts[3]  # Status is typically the 4th part
                
            return None
            
        except Exception as e:
            self.logger.error(f"Failed to get service status: {e}")
            return None
    
    def check_status(self):
        """Check and display service status"""
        try:
            print("🔍 Windows Print Service Status")
            print("=" * 40)
            
            # Check if service exists
            cmd = ["sc.exe", "query", self.service_name]
            result = subprocess.run(cmd, capture_output=True, text=True)
            
            if result.returncode == 0:
                # Parse service information
                lines = result.stdout.split('\n')
                
                service_info = {}
                for line in lines:
                    if ':' in line:
                        key, value = line.split(':', 1)
                        service_info[key.strip()] = value.strip()
                
                # Display status
                status = "Unknown"
                for line in lines:
                    if "STATE" in line:
                        parts = line.split()
                        if len(parts) >= 4:
                            status = parts[3]
                            break
                
                print(f"Service Name: {self.service_name}")
                print(f"Status: {status}")
                
                # Check if API is responding
                api_status = self._check_api_status()
                print(f"Local API: {api_status}")
                
                # Check configuration
                config_status = self._check_configuration()
                print(f"Configuration: {config_status}")
                
                print("\nService Details:")
                for key, value in service_info.items():
                    if key in ["SERVICE_NAME", "DISPLAY_NAME", "TYPE", "START_TYPE"]:
                        print(f"  {key}: {value}")
                
                # Show management commands
                print("\nManagement Commands:")
                print("  Start:     python main.py --start")
                print("  Stop:      python main.py --stop") 
                print("  Restart:   python main.py --stop && python main.py --start")
                print("  Uninstall: python main.py --uninstall")
                
            else:
                print("❌ Service is not installed")
                print("\nTo install the service:")
                print("  python main.py --install http://your-django-server:8000")
                
        except Exception as e:
            print(f"❌ Error checking service status: {e}")
    
    def _check_api_status(self) -> str:
        """Check if local API is responding"""
        try:
            import requests
            response = requests.get("http://localhost:8081/", timeout=5)
            if response.status_code == 200:
                return "✓ Running (http://localhost:8081)"
            else:
                return f"❌ Error {response.status_code}"
                
        except requests.exceptions.ConnectionError:
            return "❌ Not responding"
        except Exception as e:
            return f"❌ Error: {e}"
    
    def _check_configuration(self) -> str:
        """Check configuration status"""
        try:
            from config_manager import ConfigManager
            
            config_manager = ConfigManager()
            config = config_manager.get_config()
            
            server_url = config.get('server_url', 'Not configured')
            client_id = config.get('client_id', 'Not configured')
            
            if server_url != 'Not configured' and client_id != 'Not configured':
                return f"✓ OK (Server: {server_url})"
            else:
                return "❌ Incomplete configuration"
                
        except Exception as e:
            return f"❌ Error: {e}"
    
    def restart_service(self) -> bool:
        """Restart the service"""
        try:
            print("🔄 Restarting Windows Print Service...")
            
            if self.stop_service():
                time.sleep(2)  # Give it a moment
                if self.start_service():
                    print("✓ Service restarted successfully")
                    return True
                else:
                    print("❌ Failed to start service after stop")
                    return False
            else:
                print("❌ Failed to stop service")
                return False
                
        except Exception as e:
            print(f"❌ Service restart failed: {e}")
            return False