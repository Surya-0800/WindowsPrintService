"""
Service Manager
Main orchestrator that coordinates all service components
"""

import asyncio
import logging
import signal
import time
from typing import Optional

from config_manager import ConfigManager
from printer_manager import PrinterManager
from print_executor import PrintExecutor
from job_manager import JobManager
from api_server import APIServer

class WindowsPrintService:
    """Main service orchestrator that manages all components"""
    
    def __init__(self, config_manager: ConfigManager):
        self.config_manager = config_manager
        self.logger = logging.getLogger(__name__)
        
        # Service state
        self.running = False
        self.start_time = None
        
        # Initialize components
        self.printer_manager = None
        self.print_executor = None
        self.job_manager = None
        self.api_server = None
        
        # Tasks
        self.tasks = []
        
        self.logger.info("Windows Print Service initialized")
    
    async def initialize_components(self):
        """Initialize all service components"""
        try:
            self.logger.info("Initializing service components...")
            
            # Initialize printer manager
            self.printer_manager = PrinterManager()
            self.logger.info("✓ Printer manager initialized")
            
            # Initialize print executor
            self.print_executor = PrintExecutor(self.printer_manager)
            self.logger.info("✓ Print executor initialized")
            
            # Initialize job manager
            config = self.config_manager.get_server_config()
            self.job_manager = JobManager(config, self.print_executor, self.printer_manager)
            self.logger.info("✓ Job manager initialized")
            
            # Initialize API server
            self.api_server = APIServer(
                self.printer_manager,
                self.job_manager,
                self.config_manager
            )
            self.logger.info("✓ API server initialized")
            
            self.logger.info("✓ All components initialized successfully")
            
        except Exception as e:
            self.logger.error(f"❌ Failed to initialize components: {e}")
            raise
    
    async def start_all_tasks(self):
        """Start all service tasks concurrently"""
        try:
            self.logger.info("Starting all service tasks...")
            
            # Create tasks for all major components
            tasks = [
                asyncio.create_task(
                    self.job_manager.start_processing(),
                    name="job_processing"
                ),
                asyncio.create_task(
                    self.api_server.start_server(),
                    name="api_server"
                ),
                asyncio.create_task(
                    self._monitor_service_health(),
                    name="health_monitor"
                )
            ]
            
            self.tasks = tasks
            self.logger.info(f"Started {len(tasks)} service tasks")
            
            # Wait for all tasks to complete (or fail)
            try:
                await asyncio.gather(*tasks)
            except asyncio.CancelledError:
                self.logger.info("Service tasks cancelled")
            except Exception as e:
                self.logger.error(f"Service task error: {e}")
                raise
                
        except Exception as e:
            self.logger.error(f"Failed to start service tasks: {e}")
            raise
    
    async def _monitor_service_health(self):
        """Monitor service health and performance"""
        try:
            self.logger.info("Starting health monitor")
            
            while self.running:
                try:
                    # Log health status every 5 minutes
                    await asyncio.sleep(300)  # 5 minutes
                    
                    if self.job_manager and hasattr(self.job_manager, 'is_healthy'):
                        health = self.job_manager.is_healthy()
                        if not health:
                            self.logger.warning("Job manager health check failed")
                        else:
                            # Log performance metrics
                            metrics = self.job_manager.get_performance_metrics()
                            self.logger.info(
                                f"Service healthy - "
                                f"Polls: {metrics.get('total_polls', 0)}, "
                                f"Jobs: {metrics.get('jobs_processed', 0)}, "
                                f"Rate: {metrics.get('polls_per_second', 0):.1f}/sec"
                            )
                    
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    self.logger.error(f"Health monitor error: {e}")
                    
        except Exception as e:
            self.logger.error(f"Health monitor startup failed: {e}")
    
    async def run(self):
        """Main service entry point"""
        try:
            self.running = True
            self.start_time = time.time()
            
            self.logger.info("=" * 60)
            self.logger.info("🖨️  Windows Print Service - Ultra Fast Edition")
            self.logger.info("=" * 60)
            
            # Initialize all components
            await self.initialize_components()
            
            # Display startup information
            await self._display_startup_info()
            
            # Start all service tasks
            await self.start_all_tasks()
            
        except KeyboardInterrupt:
            self.logger.info("Service interrupted by user")
        except Exception as e:
            self.logger.error(f"Service run failed: {e}")
            raise
        finally:
            await self.stop()
    
    async def _display_startup_info(self):
        """Display service startup information"""
        try:
            config = self.config_manager.get_config()
            
            # Service information
            self.logger.info(f"Service Version: 1.0.0")
            self.logger.info(f"Client ID: {config['client_id']}")
            self.logger.info(f"Django Server: {config['server_url']}")
            self.logger.info(f"Local API: http://localhost:{config['local_api_port']}")
            self.logger.info(f"Poll Interval: {config['poll_interval']*1000:.0f}ms")
            
            # Printer information
            printers = self.printer_manager.get_printers()
            online_printers = [p for p in printers if p.get('is_online', False)]
            
            self.logger.info("-" * 40)
            self.logger.info(f"Printers Found: {len(printers)} total, {len(online_printers)} online")
            
            if online_printers:
                for printer in online_printers[:3]:  # Show first 3
                    status = "✓" if printer.get('is_online') else "✗"
                    default = " (Default)" if printer.get('is_default') else ""
                    self.logger.info(f"  {status} {printer['name']}{default}")
                
                if len(online_printers) > 3:
                    self.logger.info(f"  ... and {len(online_printers) - 3} more")
            else:
                self.logger.warning("No online printers found")
            
            # Print executor information
            tool_info = self.print_executor.get_tool_info()
            preferred_tool = tool_info.get('preferred_tool', 'system_default')
            self.logger.info(f"Print Tool: {preferred_tool}")
            
            self.logger.info("-" * 40)
            self.logger.info("🚀 Service ready for print jobs!")
            self.logger.info("Press Ctrl+C to stop the service")
            self.logger.info("=" * 60)
            
        except Exception as e:
            self.logger.error(f"Failed to display startup info: {e}")
    
    async def stop(self):
        """Stop all service components gracefully"""
        if not self.running:
            return
        
        self.running = False
        self.logger.info("Stopping Windows Print Service...")
        
        try:
            # Cancel all running tasks
            for task in self.tasks:
                if not task.done():
                    self.logger.debug(f"Cancelling task: {task.get_name()}")
                    task.cancel()
            
            # Wait for tasks to complete cancellation
            if self.tasks:
                try:
                    await asyncio.wait_for(
                        asyncio.gather(*self.tasks, return_exceptions=True),
                        timeout=10.0
                    )
                except asyncio.TimeoutError:
                    self.logger.warning("Task cancellation timeout")
            
            # Stop individual components
            if self.job_manager:
                self.job_manager.stop_processing()
            
            if self.api_server:
                await self.api_server.stop_server()
            
            # Calculate uptime
            if self.start_time:
                uptime = time.time() - self.start_time
                self.logger.info(f"Service uptime: {uptime:.1f} seconds")
            
            self.logger.info("✓ Windows Print Service stopped successfully")
            
        except Exception as e:
            self.logger.error(f"Error during service shutdown: {e}")
    
    def get_service_status(self) -> dict:
        """Get comprehensive service status"""
        try:
            status = {
                "running": self.running,
                "start_time": self.start_time,
                "uptime_seconds": time.time() - self.start_time if self.start_time else 0,
                "components": {
                    "printer_manager": self.printer_manager is not None,
                    "print_executor": self.print_executor is not None,
                    "job_manager": self.job_manager is not None,
                    "api_server": self.api_server is not None
                }
            }
            
            # Add component-specific status
            if self.job_manager:
                status["job_manager_status"] = self.job_manager.get_status()
            
            if self.printer_manager:
                status["printer_statistics"] = self.printer_manager.get_printer_statistics()
            
            return status
            
        except Exception as e:
            self.logger.error(f"Failed to get service status: {e}")
            return {"error": str(e)}