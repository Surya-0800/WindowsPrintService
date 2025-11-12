"""
Printer Manager
Handles printer detection, capabilities, and status monitoring
"""

import logging
import time
import threading
from typing import List, Dict, Optional, Any

try:
    import win32print
    import win32api
    WIN32_AVAILABLE = True
except ImportError:
    WIN32_AVAILABLE = False

class PrinterManager:
    """Manages printer detection and capabilities"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.printers: List[Dict[str, Any]] = []
        self.last_refresh = 0
        self.refresh_lock = threading.Lock()
        
        # Initialize printer list
        self.refresh_printers()
    
    def refresh_printers(self):
        """Refresh the complete printer list with capabilities"""
        with self.refresh_lock:
            try:
                self.logger.info("Refreshing printer list...")
                self.printers = []
                
                if not WIN32_AVAILABLE:
                    self._create_fallback_printers()
                    return
                
                # Get all available printers
                try:
                    printers = win32print.EnumPrinters(
                        win32print.PRINTER_ENUM_LOCAL | 
                        win32print.PRINTER_ENUM_CONNECTIONS
                    )
                    
                    self.logger.debug(f"Found {len(printers)} raw printers")
                    
                    for printer_tuple in printers:
                        printer_name = printer_tuple[2]  # Name is at index 2
                        printer_info = self._get_printer_details(printer_name)
                        
                        if printer_info:
                            self.printers.append(printer_info)
                    
                    self.last_refresh = time.time()
                    self.logger.info(f"Successfully refreshed {len(self.printers)} printers")
                    
                except Exception as e:
                    self.logger.error(f"Printer enumeration failed: {e}")
                    self._create_fallback_printers()
                    
            except Exception as e:
                self.logger.error(f"Printer refresh failed: {e}")
                self._create_fallback_printers()
    
    def _create_fallback_printers(self):
        """Create fallback printer list when win32print is not available"""
        self.printers = [
            {
                "name": "Microsoft Print to PDF",
                "display_name": "Microsoft Print to PDF",
                "is_default": True,
                "is_online": True,
                "status": "Ready",
                "printer_type": "Virtual",
                "driver_name": "Microsoft Print To PDF",
                "port_name": "PORTPROMPT:",
                "location": "",
                "comment": "Built-in PDF printer",
                "capabilities": self._get_default_capabilities()
            }
        ]
        self.logger.warning("Using fallback printer configuration (win32print not available)")
    
    def _get_printer_details(self, printer_name: str) -> Optional[Dict[str, Any]]:
        """Get comprehensive printer information"""
        try:
            # Open printer handle
            handle = win32print.OpenPrinter(printer_name)
            
            try:
                # Get printer information
                printer_info = win32print.GetPrinter(handle, 2)
                
                # Get default printer
                try:
                    default_printer = win32print.GetDefaultPrinter()
                except:
                    default_printer = ""
                
                # Determine printer status
                status_code = printer_info.get('Status', 0)
                attributes = printer_info.get('Attributes', 0)
                
                # Build comprehensive printer information
                return {
                    "name": printer_name,
                    "display_name": printer_info.get('pPrinterName', printer_name),
                    "is_default": printer_name.lower() == default_printer.lower(),
                    "is_online": self._is_printer_online(status_code, attributes),
                    "status": self._get_status_text(status_code),
                    "printer_type": self._get_printer_type(attributes),
                    "driver_name": printer_info.get('pDriverName', 'Unknown'),
                    "port_name": printer_info.get('pPortName', 'Unknown'),
                    "location": printer_info.get('pLocation', ''),
                    "comment": printer_info.get('pComment', ''),
                    "server_name": printer_info.get('pServerName', ''),
                    "capabilities": self._get_printer_capabilities(printer_name)
                }
                
            finally:
                win32print.ClosePrinter(handle)
                
        except Exception as e:
            self.logger.warning(f"Failed to get details for printer '{printer_name}': {e}")
            
            # Return minimal information as fallback
            return {
                "name": printer_name,
                "display_name": printer_name,
                "is_default": False,
                "is_online": True,  # Assume online if we can't determine
                "status": "Unknown",
                "printer_type": "Unknown",
                "driver_name": "Unknown",
                "port_name": "Unknown",
                "location": "",
                "comment": "",
                "server_name": "",
                "capabilities": self._get_default_capabilities()
            }
    
    def _is_printer_online(self, status_code: int, attributes: int) -> bool:
        """Determine if printer is online"""
        # Status codes: 0=Ready, 3=Idle, 8=Offline
        if status_code in [0, 3]:
            return True
        elif status_code == 8:
            return False
        
        # For network printers, be optimistic
        try:
            if attributes & win32print.PRINTER_ATTRIBUTE_NETWORK:
                return status_code not in [8, 13]  # Not offline or unavailable
        except:
            pass
        
        # Default assumption
        return status_code not in [2, 8, 13]  # Not error, offline, or unavailable
    
    def _get_status_text(self, status_code: int) -> str:
        """Convert status code to human-readable text"""
        status_map = {
            0: "Ready",
            1: "Paused",
            2: "Error", 
            3: "Idle",
            4: "Paper Jam",
            5: "Paper Out",
            6: "Manual Feed Required",
            7: "Paper Problem",
            8: "Offline",
            9: "I/O Active",
            10: "Busy",
            11: "Printing",
            12: "Output Bin Full",
            13: "Not Available",
            14: "Waiting",
            15: "Processing",
            16: "Initializing",
            17: "Warming Up"
        }
        return status_map.get(status_code, f"Status {status_code}")
    
    def _get_printer_type(self, attributes: int) -> str:
        """Determine printer type from attributes"""
        try:
            if attributes & win32print.PRINTER_ATTRIBUTE_NETWORK:
                return "Network"
            elif attributes & win32print.PRINTER_ATTRIBUTE_LOCAL:
                return "Local"
            elif attributes & win32print.PRINTER_ATTRIBUTE_SHARED:
                return "Shared"
            else:
                return "Virtual"
        except:
            return "Unknown"
    
    def _get_printer_capabilities(self, printer_name: str) -> Dict[str, Any]:
        """Get printer capabilities and supported features"""
        try:
            # For now, return comprehensive default capabilities
            # In a full implementation, this would query actual printer capabilities
            capabilities = {
                "paper_sizes": [
                    {"name": "A4", "width": 210, "height": 297, "units": "mm"},
                    {"name": "Letter", "width": 8.5, "height": 11, "units": "inches"},
                    {"name": "Legal", "width": 8.5, "height": 14, "units": "inches"},
                    {"name": "A3", "width": 297, "height": 420, "units": "mm"}
                ],
                "orientations": ["Portrait", "Landscape"],
                "color_modes": self._detect_color_support(printer_name),
                "print_qualities": ["Draft", "Normal", "High", "Best"],
                "duplex_modes": self._detect_duplex_support(printer_name),
                "scaling_options": {
                    "auto_scale": True,
                    "fit_to_page": True,
                    "custom_scale": True,
                    "scale_range": {"min": 25, "max": 400}
                },
                "advanced_features": {
                    "collation": True,
                    "multiple_copies": True,
                    "page_ranges": True,
                    "borderless": self._detect_borderless_support(printer_name)
                }
            }
            
            return capabilities
            
        except Exception as e:
            self.logger.warning(f"Failed to get capabilities for {printer_name}: {e}")
            return self._get_default_capabilities()
    
    def _detect_color_support(self, printer_name: str) -> List[str]:
        """Detect color support based on printer name and type"""
        name_lower = printer_name.lower()
        
        if any(keyword in name_lower for keyword in ['color', 'colour', 'inkjet']):
            return ["Color", "Grayscale", "Black and White"]
        elif any(keyword in name_lower for keyword in ['laser', 'mono']):
            return ["Black and White", "Grayscale"]
        else:
            # Default to color support
            return ["Color", "Grayscale", "Black and White"]
    
    def _detect_duplex_support(self, printer_name: str) -> List[str]:
        """Detect duplex support"""
        name_lower = printer_name.lower()
        
        if any(keyword in name_lower for keyword in ['duplex', 'double-sided']):
            return ["None", "Long Edge", "Short Edge"]
        else:
            return ["None"]
    
    def _detect_borderless_support(self, printer_name: str) -> bool:
        """Detect borderless printing support"""
        name_lower = printer_name.lower()
        return any(keyword in name_lower for keyword in ['photo', 'inkjet', 'borderless'])
    
    def _get_default_capabilities(self) -> Dict[str, Any]:
        """Get default capabilities for fallback"""
        return {
            "paper_sizes": [
                {"name": "A4", "width": 210, "height": 297, "units": "mm"},
                {"name": "Letter", "width": 8.5, "height": 11, "units": "inches"}
            ],
            "orientations": ["Portrait", "Landscape"],
            "color_modes": ["Color", "Grayscale", "Black and White"],
            "print_qualities": ["Draft", "Normal", "High"],
            "duplex_modes": ["None"],
            "scaling_options": {
                "auto_scale": True,
                "fit_to_page": True,
                "custom_scale": False,
                "scale_range": {"min": 100, "max": 100}
            },
            "advanced_features": {
                "collation": True,
                "multiple_copies": True,
                "page_ranges": True,
                "borderless": False
            }
        }
    
    # Public interface methods
    def get_printers(self) -> List[Dict[str, Any]]:
        """Get list of all printers (auto-refresh if stale)"""
        # Auto-refresh if data is older than 5 minutes
        if time.time() - self.last_refresh > 300:
            self.refresh_printers()
        
        return self.printers.copy()
    
    def get_printer_by_name(self, name: str) -> Optional[Dict[str, Any]]:
        """Get specific printer by name (case-insensitive)"""
        for printer in self.get_printers():
            if printer['name'].lower() == name.lower():
                return printer
        return None
    
    def is_printer_available(self, name: str) -> bool:
        """Check if printer exists and is online"""
        printer = self.get_printer_by_name(name)
        return printer is not None and printer.get('is_online', False)
    
    def get_default_printer(self) -> Optional[Dict[str, Any]]:
        """Get the default printer"""
        for printer in self.get_printers():
            if printer.get('is_default', False):
                return printer
        
        # If no default found, return first available online printer
        online_printers = [p for p in self.get_printers() if p.get('is_online', False)]
        return online_printers[0] if online_printers else None
    
    def get_online_printers(self) -> List[Dict[str, Any]]:
        """Get only online printers"""
        return [p for p in self.get_printers() if p.get('is_online', False)]
    
    def get_printer_statistics(self) -> Dict[str, Any]:
        """Get printer statistics"""
        printers = self.get_printers()
        online_printers = self.get_online_printers()
        
        stats = {
            "total_printers": len(printers),
            "online_printers": len(online_printers),
            "offline_printers": len(printers) - len(online_printers),
            "default_printer": None,
            "printer_types": {},
            "last_refresh": self.last_refresh
        }
        
        # Find default printer
        for printer in printers:
            if printer.get('is_default', False):
                stats["default_printer"] = printer['name']
                break
        
        # Count by type
        for printer in printers:
            ptype = printer.get('printer_type', 'Unknown')
            stats['printer_types'][ptype] = stats['printer_types'].get(ptype, 0) + 1
        
        return stats