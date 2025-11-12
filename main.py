#!/usr/bin/env python3
"""
Windows Print Service - Main Entry Point
Ultra-fast print service for web applications with zero-dialog printing
"""

import sys
import os
import asyncio
import argparse
import logging
import signal
import time
from pathlib import Path

# Add src directory to Python path
current_dir = Path(__file__).parent.absolute()
src_dir = current_dir / "src"
if str(src_dir) not in sys.path:
    sys.path.insert(0, str(src_dir))

def setup_logging():
    """Setup basic logging configuration"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler('service.log', encoding='utf-8')
        ]
    )

def main():
    """Main entry point"""
    setup_logging()
    logger = logging.getLogger(__name__)
    
    parser = argparse.ArgumentParser(description='Windows Print Service')
    parser.add_argument('--install', metavar='SERVER_URL', help='Install Windows service')
    parser.add_argument('--uninstall', action='store_true', help='Uninstall Windows service')
    parser.add_argument('--start', action='store_true', help='Start Windows service')
    parser.add_argument('--stop', action='store_true', help='Stop Windows service')
    parser.add_argument('--status', action='store_true', help='Check service status')
    parser.add_argument('--console', action='store_true', help='Run in console mode')
    
    args = parser.parse_args()
    
    try:
        # Import modules from src directory
        from src.service_installer import ServiceInstaller
        from src.service_manager import WindowsPrintService
        from src.config_manager import ConfigManager
        
        # Handle service management commands
        if args.install:
            installer = ServiceInstaller()
            success = installer.install_service(args.install)
            return 0 if success else 1
            
        if args.uninstall:
            installer = ServiceInstaller()
            success = installer.uninstall_service()
            return 0 if success else 1
            
        if args.start:
            installer = ServiceInstaller()
            success = installer.start_service()
            return 0 if success else 1
            
        if args.stop:
            installer = ServiceInstaller()
            success = installer.stop_service()
            return 0 if success else 1
            
        if args.status:
            installer = ServiceInstaller()
            installer.check_status()
            return 0
        
        # Default: run service in console mode
        print("🖨️  Windows Print Service - Ultra Fast Edition")
        print("=" * 50)
        print(f"Machine: {os.environ.get('COMPUTERNAME', 'Unknown')}")
        print(f"User: {os.environ.get('USERNAME', 'Unknown')}")
        print("Press Ctrl+C to stop the service")
        print()
        
        # Load configuration
        config_manager = ConfigManager()
        config = config_manager.get_config()
        
        print(f"Django Server: {config['server_url']}")
        print(f"Client ID: {config['client_id']}")
        print(f"Local API: http://localhost:{config['local_api_port']}")
        print(f"Poll Interval: {config['poll_interval']*1000:.0f}ms")
        print()
        
        # Create service instance
        service = WindowsPrintService(config_manager)
        
        # Setup signal handlers for graceful shutdown
        def signal_handler(signum, frame):
            logger.info(f"Received signal {signum}, shutting down...")
            service.stop()
            
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        # Run the service
        asyncio.run(service.run())
        return 0
        
    except KeyboardInterrupt:
        print("\nService stopped by user")
        return 0
    except ImportError as e:
        print(f"Import error: {e}")
        print("Make sure all required packages are installed: pip install -r requirements.txt")
        return 1
    except Exception as e:
        logger.exception("Fatal error")
        print(f"Fatal error: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())