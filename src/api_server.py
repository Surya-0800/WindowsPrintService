"""
FastAPI Local Server
Provides REST API for printer management and print job submission
Fixed version with better error handling and startup verification
"""

from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional
import logging
import time
import asyncio
import socket

# Request models
class PrintJobRequest(BaseModel):
    content_type: str = Field(..., description="Content type: pdf, base64_pdf, url")
    content: Optional[str] = Field(None, description="Base64 PDF content")
    content_url: Optional[str] = Field(None, description="URL to PDF content")
    printer_name: str = Field(..., description="Target printer name")
    settings: Optional[Dict[str, Any]] = Field(default_factory=dict, description="Print settings")

class MultiPrinterRequest(BaseModel):
    document_url: str = Field(..., description="URL to PDF document")
    printer_assignments: List[Dict[str, Any]] = Field(..., description="Printer assignments")

class ConfigUpdateRequest(BaseModel):
    updates: Dict[str, Any] = Field(..., description="Configuration updates")

def create_api_app(printer_manager, job_manager, config_manager) -> FastAPI:
    """Create FastAPI application with all endpoints"""
    
    app = FastAPI(
        title="Windows Print Service API",
        description="Ultra-fast local printing service",
        version="1.0.0"
    )
    
    logger = logging.getLogger(__name__)
    
    # Add CORS middleware
    config = config_manager.get_config()
    if config.get('enable_cors', True):
        app.add_middleware(
            CORSMiddleware,
            allow_origins=["*"],
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )
    
    # Health check endpoint
    @app.get("/", summary="Health Check")
    async def root():
        """Health check endpoint"""
        return {
            "service": "Windows Print Service",
            "status": "running",
            "version": "1.0.0",
            "timestamp": time.time(),
            "message": "API is working correctly!"
        }
    
    # Printer management endpoints
    @app.get("/api/printers", summary="Get All Printers")
    async def get_printers():
        """Get list of all available printers with capabilities"""
        try:
            printers = printer_manager.get_printers()
            return {
                "status": "success",
                "printers": printers,
                "count": len(printers)
            }
        except Exception as e:
            logger.error(f"Failed to get printers: {e}")
            raise HTTPException(status_code=500, detail="Failed to retrieve printers")
    
    @app.post("/api/printers/refresh", summary="Refresh Printer List")
    async def refresh_printers():
        """Refresh the printer list"""
        try:
            printer_manager.refresh_printers()
            printers = printer_manager.get_printers()
            return {
                "status": "success",
                "message": "Printers refreshed successfully",
                "count": len(printers)
            }
        except Exception as e:
            logger.error(f"Failed to refresh printers: {e}")
            raise HTTPException(status_code=500, detail="Failed to refresh printers")
    
    @app.get("/api/printers/{printer_name}", summary="Get Specific Printer")
    async def get_printer_details(printer_name: str):
        """Get detailed information about a specific printer"""
        try:
            printer = printer_manager.get_printer_by_name(printer_name)
            if not printer:
                raise HTTPException(status_code=404, detail=f"Printer '{printer_name}' not found")
            
            return {
                "status": "success",
                "printer": printer
            }
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Failed to get printer details: {e}")
            raise HTTPException(status_code=500, detail="Failed to retrieve printer details")
    
    # Print job endpoints
    @app.post("/api/print", summary="Submit Print Job")
    async def submit_print_job(request: PrintJobRequest):
        """Submit a single print job for immediate processing"""
        try:
            # Validate printer
            if not printer_manager.is_printer_available(request.printer_name):
                raise HTTPException(
                    status_code=400, 
                    detail=f"Printer '{request.printer_name}' is not available"
                )
            
            # Create job structure
            job = {
                "id": f"local_{int(time.time() * 1000)}",
                "content_type": request.content_type,
                "content": request.content,
                "content_url": request.content_url,
                "printer_name": request.printer_name,
                "settings": request.settings or {}
            }
            
            # Execute print job directly (local processing)
            success = await job_manager.print_executor.execute_print_job(job)
            
            if success:
                return {
                    "status": "success",
                    "job_id": job["id"],
                    "message": "Print job completed successfully"
                }
            else:
                raise HTTPException(status_code=500, detail="Print job failed")
                
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Print job submission failed: {e}")
            raise HTTPException(status_code=500, detail=f"Print job failed: {str(e)}")
    
    @app.post("/api/print-multi", summary="Submit Multi-Printer Job")
    async def submit_multi_printer_job(request: MultiPrinterRequest):
        """Submit a job that prints different pages to different printers"""
        try:
            # Validate all printers
            for assignment in request.printer_assignments:
                printer_name = assignment.get('printer_name')
                if not printer_name:
                    raise HTTPException(status_code=400, detail="Missing printer_name in assignment")
                
                if not printer_manager.is_printer_available(printer_name):
                    raise HTTPException(
                        status_code=400, 
                        detail=f"Printer '{printer_name}' is not available"
                    )
            
            # Execute multi-printer job
            success = await job_manager.print_executor.execute_multi_printer_job(
                request.document_url, 
                request.printer_assignments
            )
            
            if success:
                return {
                    "status": "success",
                    "message": f"Multi-printer job completed successfully",
                    "assignments_count": len(request.printer_assignments)
                }
            else:
                raise HTTPException(status_code=500, detail="Multi-printer job failed")
                
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Multi-printer job failed: {e}")
            raise HTTPException(status_code=500, detail=f"Multi-printer job failed: {str(e)}")
    
    # Service management endpoints
    @app.get("/api/status", summary="Get Service Status")
    async def get_service_status():
        """Get comprehensive service status"""
        try:
            job_status = job_manager.get_status()
            printer_stats = printer_manager.get_printer_statistics()
            
            return {
                "status": "success",
                "service_info": {
                    "running": True,
                    "version": "1.0.0",
                    "uptime_seconds": time.time() - job_status.get('last_successful_contact', time.time())
                },
                "job_manager": job_status,
                "printer_manager": printer_stats,
                "health": {
                    "overall": job_manager.is_healthy(),
                    "job_processing": job_status.get('healthy', False),
                    "printer_detection": len(printer_stats.get('online_printers', 0)) > 0
                }
            }
        except Exception as e:
            logger.error(f"Failed to get service status: {e}")
            raise HTTPException(status_code=500, detail="Failed to retrieve service status")
    
    return app

class APIServer:
    """Manages the FastAPI server lifecycle with better error handling"""
    
    def __init__(self, printer_manager, job_manager, config_manager):
        self.printer_manager = printer_manager
        self.job_manager = job_manager
        self.config_manager = config_manager
        self.logger = logging.getLogger(__name__)
        self.app = None
        self.server = None
        self.server_task = None
    
    def _check_port_available(self, port: int) -> bool:
        """Check if port is available"""
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.bind(('127.0.0.1', port))
                return True
        except OSError:
            return False
    
    async def start_server(self):
        """Start the FastAPI server with better error handling"""
        try:
            # Create FastAPI app
            self.app = create_api_app(
                self.printer_manager,
                self.job_manager,
                self.config_manager
            )
            
            # Get configuration
            config = self.config_manager.get_api_config()
            port = config.get('local_api_port', 8081)
            
            # Check if port is available
            if not self._check_port_available(port):
                self.logger.error(f"Port {port} is already in use!")
                raise Exception(f"Port {port} is not available")
            
            self.logger.info(f"Starting API server on http://127.0.0.1:{port}")
            
            # Import uvicorn here to avoid startup issues
            import uvicorn
            
            # Configure uvicorn with more specific settings
            server_config = uvicorn.Config(
                self.app,
                host="127.0.0.1",
                port=port,
                log_level="error",  # Reduce uvicorn logging
                access_log=False,   # Disable access logs
                loop="asyncio"      # Specify event loop
            )
            
            self.server = uvicorn.Server(server_config)
            
            # Start server and wait for it to be ready
            self.server_task = asyncio.create_task(self.server.serve())
            
            # Give server a moment to start
            await asyncio.sleep(0.5)
            
            # Verify server is actually running
            await self._verify_server_running(port)
            
            self.logger.info(f"✓ API server started successfully on http://127.0.0.1:{port}")
            
            # Wait for the server task to complete
            await self.server_task
            
        except Exception as e:
            self.logger.error(f"❌ API server startup failed: {e}")
            raise
    
    async def _verify_server_running(self, port: int):
        """Verify that the server is actually responding"""
        import aiohttp
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(f"http://127.0.0.1:{port}/", timeout=aiohttp.ClientTimeout(total=5)) as response:
                    if response.status == 200:
                        self.logger.info("✓ API server verification successful")
                        return True
        except Exception as e:
            self.logger.error(f"❌ API server verification failed: {e}")
            raise Exception(f"Server started but not responding: {e}")
    
    async def stop_server(self):
        """Stop the FastAPI server"""
        try:
            if self.server:
                self.logger.info("Stopping API server...")
                self.server.should_exit = True
                
                if self.server_task and not self.server_task.done():
                    self.server_task.cancel()
                    try:
                        await asyncio.wait_for(self.server_task, timeout=5.0)
                    except (asyncio.TimeoutError, asyncio.CancelledError):
                        pass
                
                self.logger.info("✓ API server stopped")
        except Exception as e:
            self.logger.error(f"Error stopping API server: {e}")