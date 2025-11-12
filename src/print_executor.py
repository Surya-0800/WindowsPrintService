"""
Print Executor
Optimized for fast performance with concurrent multi-printer support
"""

import os
import subprocess
import tempfile
import base64
import asyncio
import logging
import time
import aiohttp
import aiofiles
from pathlib import Path
from typing import Dict, Any, Optional, List

class PrintExecutor:
    """Executes print jobs with optimized performance and concurrent multi-printer support"""
    
    # Class-level cache for tool paths (shared across instances)
    _tool_cache = {
        'sumatra': None,
        'adobe': None,
        'pdftk': None,
        'ghostscript': None,
        'initialized': False
    }
    
    # Class-level semaphore for concurrent print job limit
    _print_semaphore = asyncio.Semaphore(10)  # Max 10 concurrent prints
    
    def __init__(self, printer_manager):
        self.printer_manager = printer_manager
        self.logger = logging.getLogger(__name__)
        
        # Performance tracking
        self.jobs_processed = 0
        self.successful_jobs = 0
        self.total_processing_time = 0.0
        
        # Initialize tools asynchronously if not already done
        if not PrintExecutor._tool_cache['initialized']:
            asyncio.create_task(self._initialize_tools())
        else:
            self.sumatra_path = PrintExecutor._tool_cache['sumatra']
            self.adobe_path = PrintExecutor._tool_cache['adobe']
            self.pdftk_path = PrintExecutor._tool_cache['pdftk']
            self.ghostscript_path = PrintExecutor._tool_cache['ghostscript']
            self.preferred_tool = self._select_preferred_tool()
            
            if self.logger.isEnabledFor(logging.INFO):
                self.logger.info(f"Print executor initialized (cached) with tool: {self.preferred_tool or 'system default'}")
    
    async def _initialize_tools(self):
        """Asynchronously initialize all tools in parallel"""
        try:
            results = await asyncio.gather(
                self._find_sumatra_pdf_async(),
                self._find_adobe_reader_async(),
                self._find_pdftk_async(),
                self._find_ghostscript_async(),
                return_exceptions=True
            )
            
            self.sumatra_path = results[0] if not isinstance(results[0], Exception) else None
            self.adobe_path = results[1] if not isinstance(results[1], Exception) else None
            self.pdftk_path = results[2] if not isinstance(results[2], Exception) else None
            self.ghostscript_path = results[3] if not isinstance(results[3], Exception) else None
            
            PrintExecutor._tool_cache.update({
                'sumatra': self.sumatra_path,
                'adobe': self.adobe_path,
                'pdftk': self.pdftk_path,
                'ghostscript': self.ghostscript_path,
                'initialized': True
            })
            
            self.preferred_tool = self._select_preferred_tool()
            
            if self.logger.isEnabledFor(logging.INFO):
                self.logger.info(f"Print executor initialized with tool: {self.preferred_tool or 'system default'}")
                
        except Exception as e:
            self.logger.error(f"Tool initialization error: {e}")
    
    async def _find_sumatra_pdf_async(self) -> Optional[str]:
        """Find SumatraPDF installation asynchronously"""
        possible_paths = [
            r"C:\Program Files\SumatraPDF\SumatraPDF.exe",
            r"C:\Program Files (x86)\SumatraPDF\SumatraPDF.exe",
            os.path.join(os.path.expanduser("~"), "AppData", "Local", "SumatraPDF", "SumatraPDF.exe"),
            Path(__file__).parent / "tools" / "SumatraPDF.exe"
        ]
        
        for path in possible_paths:
            if os.path.exists(str(path)):
                if self.logger.isEnabledFor(logging.INFO):
                    self.logger.info(f"Found SumatraPDF: {path}")
                return str(path)
        
        self.logger.debug("SumatraPDF not found")
        return None
    
    async def _find_adobe_reader_async(self) -> Optional[str]:
        """Find Adobe Reader installation asynchronously"""
        possible_paths = [
            r"C:\Program Files (x86)\Adobe\Acrobat Reader DC\Reader\AcroRd32.exe",
            r"C:\Program Files\Adobe\Acrobat Reader DC\Reader\AcroRd32.exe",
            r"C:\Program Files (x86)\Adobe\Reader 11.0\Reader\AcroRd32.exe",
            r"C:\Program Files\Adobe\Reader 11.0\Reader\AcroRd32.exe"
        ]
        
        for path in possible_paths:
            if os.path.exists(path):
                if self.logger.isEnabledFor(logging.INFO):
                    self.logger.info(f"Found Adobe Reader: {path}")
                return path
        
        self.logger.debug("Adobe Reader not found")
        return None
    
    async def _find_pdftk_async(self) -> Optional[str]:
        """Find PDFtk asynchronously"""
        possible_paths = [
            r"C:\Program Files (x86)\PDFtk\bin\pdftk.exe",
            r"C:\Program Files\PDFtk\bin\pdftk.exe",
            "pdftk.exe"
        ]
        
        for path in possible_paths:
            try:
                process = await asyncio.create_subprocess_exec(
                    path, "--version",
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE
                )
                await asyncio.wait_for(process.wait(), timeout=2)
                if process.returncode == 0:
                    return path
            except:
                continue
        
        return None
    
    async def _find_ghostscript_async(self) -> Optional[str]:
        """Find Ghostscript asynchronously"""
        possible_paths = [
            r"C:\Program Files\gs\gs*\bin\gswin64c.exe",
            r"C:\Program Files (x86)\gs\gs*\bin\gswin32c.exe",
            "gs"
        ]
        
        for path_pattern in possible_paths:
            if "*" in path_pattern:
                import glob
                matches = glob.glob(path_pattern)
                for path in matches:
                    try:
                        process = await asyncio.create_subprocess_exec(
                            path, "--version",
                            stdout=asyncio.subprocess.PIPE,
                            stderr=asyncio.subprocess.PIPE
                        )
                        await asyncio.wait_for(process.wait(), timeout=2)
                        if process.returncode == 0:
                            return path
                    except:
                        continue
            else:
                try:
                    process = await asyncio.create_subprocess_exec(
                        path_pattern, "--version",
                        stdout=asyncio.subprocess.PIPE,
                        stderr=asyncio.subprocess.PIPE
                    )
                    await asyncio.wait_for(process.wait(), timeout=2)
                    if process.returncode == 0:
                        return path_pattern
                except:
                    continue
        
        return None
    
    def _select_preferred_tool(self) -> Optional[str]:
        """Select the best available PDF printing tool"""
        if self.sumatra_path:
            return "sumatra"
        elif self.adobe_path:
            return "adobe"
        else:
            self.logger.warning("No specialized PDF tools found, will use system default")
            return None
    
    async def execute_batch_print_jobs(self, jobs: List[Dict[str, Any]]) -> Dict[str, bool]:
        """
        Execute multiple print jobs concurrently for maximum speed
        
        Args:
            jobs: List of print job dictionaries
            
        Returns:
            Dict mapping job IDs to success status
        """
        start_time = time.time()
        
        if self.logger.isEnabledFor(logging.INFO):
            self.logger.info(f"Executing batch of {len(jobs)} print jobs concurrently")
        
        # Execute all jobs concurrently
        tasks = []
        for job in jobs:
            task = self.execute_print_job(job)
            tasks.append(task)
        
        # Wait for all jobs to complete
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Build result dictionary
        result_dict = {}
        for job, result in zip(jobs, results):
            job_id = job.get('id', 'unknown')
            success = result is True if not isinstance(result, Exception) else False
            result_dict[job_id] = success
            
            if isinstance(result, Exception):
                self.logger.error(f"Job {job_id} failed with exception: {result}")
        
        total_time = time.time() - start_time
        successful = sum(1 for v in result_dict.values() if v)
        
        if self.logger.isEnabledFor(logging.INFO):
            self.logger.info(
                f"Batch print completed: {successful}/{len(jobs)} successful "
                f"in {total_time*1000:.0f}ms (avg {total_time*1000/len(jobs):.0f}ms per job)"
            )
        
        return result_dict
    
    async def execute_print_job(self, job: Dict[str, Any]) -> bool:
        """Execute a complete print job with page range and cropping support"""
        start_time = time.time()
        job_id = job.get('id', 'unknown')
        
        # Use semaphore to limit concurrent prints (prevents printer queue overload)
        async with PrintExecutor._print_semaphore:
            try:
                if self.logger.isEnabledFor(logging.INFO):
                    self.logger.info(f"Executing print job {job_id}")
                
                if not self._validate_print_job(job):
                    return False
                
                pdf_path = await self._prepare_pdf_content(job)
                if not pdf_path:
                    self.logger.error(f"Failed to prepare PDF content for job {job_id}")
                    return False
                
                try:
                    settings = job.get('settings', {})
                    page_range = settings.get('page_range', '').strip()
                    crop_settings = settings.get('crop', {})
                    page_orientations = settings.get('page_orientations', {})
                    
                    final_pdf_path = pdf_path
                    
                    if page_range and page_range.lower() != 'all' and page_range != '':
                        if self.logger.isEnabledFor(logging.INFO):
                            self.logger.info(f"Extracting pages: {page_range}")
                        extracted_pdf = await self._extract_pages(pdf_path, page_range)
                        
                        if extracted_pdf:
                            final_pdf_path = extracted_pdf
                            if self.logger.isEnabledFor(logging.INFO):
                                self.logger.info(f"Page extraction successful: {page_range}")
                        else:
                            self.logger.warning(f"Page extraction failed, printing original PDF")
                    
                    if crop_settings:
                        if self.logger.isEnabledFor(logging.INFO):
                            self.logger.info(f"Applying crop settings: {crop_settings}")
                        cropped_pdf = await self._apply_crop_settings(final_pdf_path, crop_settings)
                        
                        if cropped_pdf:
                            if final_pdf_path != pdf_path:
                                self._cleanup_temp_file(final_pdf_path)
                            final_pdf_path = cropped_pdf
                            if self.logger.isEnabledFor(logging.INFO):
                                self.logger.info(f"Crop settings applied successfully")
                        else:
                            self.logger.warning(f"Crop settings failed, using uncropped PDF")
                    
                    if page_orientations:
                        if self.logger.isEnabledFor(logging.INFO):
                            self.logger.info(f"Processing per-page orientations: {page_orientations}")
                        success = await self._print_with_page_orientations(final_pdf_path, job, page_orientations)
                    else:
                        print_settings = settings.copy()
                        print_settings.pop('page_range', None)
                        print_settings.pop('crop', None)
                        
                        job_for_printing = job.copy()
                        job_for_printing['settings'] = print_settings
                        
                        success = await self._execute_print(final_pdf_path, job_for_printing)
                    
                    if final_pdf_path != pdf_path:
                        self._cleanup_temp_file(final_pdf_path)
                    
                    processing_time = time.time() - start_time
                    self.jobs_processed += 1
                    self.total_processing_time += processing_time
                    
                    if success:
                        self.successful_jobs += 1
                    
                    if self.logger.isEnabledFor(logging.INFO):
                        self.logger.info(
                            f"Job {job_id} {'completed' if success else 'failed'} "
                            f"in {processing_time*1000:.0f}ms"
                        )
                    
                    return success
                    
                finally:
                    self._cleanup_temp_file(pdf_path)
                    
            except Exception as e:
                self.logger.error(f"Print job execution failed for {job_id}: {e}")
                return False

    async def _print_with_page_orientations(self, pdf_path: str, job: Dict[str, Any], page_orientations: Dict[str, str]) -> bool:
        """Print PDF with different orientations for different pages"""
        try:
            if self.logger.isEnabledFor(logging.INFO):
                self.logger.info(f"Printing with per-page orientations: {page_orientations}")
            
            page_count = await self._get_pdf_page_count(pdf_path)
            if not page_count:
                self.logger.error("Could not determine PDF page count")
                return False
            
            if self.logger.isEnabledFor(logging.INFO):
                self.logger.info(f"PDF has {page_count} pages")
            
            orientation_groups = {}
            for page_num in range(1, page_count + 1):
                page_key = str(page_num)
                orientation = page_orientations.get(page_key, job.get('settings', {}).get('orientation', 'portrait'))
                
                if orientation not in orientation_groups:
                    orientation_groups[orientation] = []
                orientation_groups[orientation].append(page_num)
            
            if self.logger.isEnabledFor(logging.INFO):
                self.logger.info(f"Orientation groups: {orientation_groups}")
            
            # Execute all orientation groups concurrently
            tasks = []
            for orientation, pages in orientation_groups.items():
                task = self._print_orientation_group(pdf_path, job, orientation, pages)
                tasks.append(task)
            
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            all_successful = all(result is True for result in results if not isinstance(result, Exception))
            
            return all_successful
            
        except Exception as e:
            self.logger.error(f"Per-page orientation printing failed: {e}")
            return False
    
    async def _print_orientation_group(self, pdf_path: str, job: Dict[str, Any], orientation: str, pages: List[int]) -> bool:
        """Print a group of pages with the same orientation"""
        try:
            if self.logger.isEnabledFor(logging.INFO):
                self.logger.info(f"Printing pages {pages} with orientation: {orientation}")
            
            page_ranges = []
            for page in pages:
                page_ranges.append(str(page))
            page_range_str = ",".join(page_ranges)
            
            extracted_pdf = await self._extract_pages(pdf_path, page_range_str)
            if not extracted_pdf:
                self.logger.error(f"Failed to extract pages {pages} for orientation {orientation}")
                return False
            
            try:
                orientation_settings = job.get('settings', {}).copy()
                orientation_settings['orientation'] = orientation
                orientation_settings.pop('page_orientations', None)
                orientation_settings.pop('page_range', None)
                
                orientation_job = job.copy()
                orientation_job['settings'] = orientation_settings
                
                success = await self._execute_print(extracted_pdf, orientation_job)
                if not success:
                    self.logger.error(f"Failed to print pages {pages} with orientation {orientation}")
                    return False
                else:
                    if self.logger.isEnabledFor(logging.INFO):
                        self.logger.info(f"Successfully printed pages {pages} with orientation {orientation}")
                    return True
                    
            finally:
                self._cleanup_temp_file(extracted_pdf)
                
        except Exception as e:
            self.logger.error(f"Orientation group printing failed: {e}")
            return False
    
    # [Keep all the other methods from the previous optimized version - _apply_crop_settings, _crop_keep_top, etc.]
    # I'll include the critical path methods here for completeness:
    
    async def _apply_crop_settings(self, pdf_path: str, crop_settings: Dict[str, Any]) -> Optional[str]:
        """Apply cropping/positioning settings to PDF"""
        try:
            crop_method = crop_settings.get('method', 'auto')
            
            if crop_method == 'keep_top':
                return await self._crop_keep_top(pdf_path, crop_settings)
            elif crop_method == 'keep_bottom':
                return await self._crop_keep_bottom(pdf_path, crop_settings)
            elif crop_method == 'custom':
                return await self._crop_custom(pdf_path, crop_settings)
            else:
                if crop_settings.get('keep_top', False) or 'keep_top_percent' in crop_settings:
                    return await self._crop_keep_top(pdf_path, crop_settings)
                elif crop_settings.get('keep_bottom', False) or 'keep_bottom_percent' in crop_settings:
                    return await self._crop_keep_bottom(pdf_path, crop_settings)
                elif 'crop_box' in crop_settings:
                    return await self._crop_custom(pdf_path, crop_settings)
                else:
                    self.logger.warning("No valid crop method detected")
                    return None
                    
        except Exception as e:
            self.logger.error(f"Crop settings application failed: {e}")
            return None
    
    async def _crop_keep_top(self, pdf_path: str, crop_settings: Dict[str, Any]) -> Optional[str]:
        """Crop PDF to keep top portion and remove bottom"""
        try:
            return await self._crop_keep_top_python(pdf_path, crop_settings)
        except Exception as e:
            self.logger.error(f"Keep-top cropping failed: {e}")
            return None
    
    async def _crop_keep_top_python(self, pdf_path: str, crop_settings: Dict[str, Any]) -> Optional[str]:
        """Use Python PDF library to keep top portion"""
        try:
            import PyPDF2 as pdf_lib
            
            crop_height_percent = crop_settings.get('keep_top_percent', 80)
            
            async with aiofiles.open(pdf_path, 'rb') as input_file:
                content = await input_file.read()
                
            from io import BytesIO
            pdf_reader = pdf_lib.PdfReader(BytesIO(content))
            pdf_writer = pdf_lib.PdfWriter()
            
            for page in pdf_reader.pages:
                mediabox = page.mediabox
                
                left = float(mediabox.left)
                bottom = float(mediabox.bottom)
                right = float(mediabox.right)
                top = float(mediabox.top)
                
                width = right - left
                height = top - bottom
                new_height = height * (crop_height_percent / 100.0)
                
                page.cropbox.lower_left = (left, top - new_height)
                page.cropbox.upper_right = (right, top)
                
                pdf_writer.add_page(page)
            
            output_file = tempfile.NamedTemporaryFile(
                suffix='_cropped.pdf', 
                delete=False,
                prefix="cropped_top_"
            )
            
            pdf_writer.write(output_file)
            output_file.close()
            
            if os.path.exists(output_file.name) and os.path.getsize(output_file.name) > 0:
                if self.logger.isEnabledFor(logging.INFO):
                    self.logger.info(f"Python top-crop successful (keep {crop_height_percent}%)")
                return output_file.name
            else:
                self.logger.error("Python top-crop produced empty file")
                self._cleanup_temp_file(output_file.name)
                return None
                    
        except Exception as e:
            self.logger.error(f"Python top-crop error: {e}")
            return None
    
    async def _crop_keep_bottom(self, pdf_path: str, crop_settings: Dict[str, Any]) -> Optional[str]:
        """Crop PDF to keep bottom portion"""
        try:
            return await self._crop_keep_bottom_python(pdf_path, crop_settings)
        except Exception as e:
            self.logger.error(f"Keep-bottom cropping failed: {e}")
            return None
    
    async def _crop_keep_bottom_python(self, pdf_path: str, crop_settings: Dict[str, Any]) -> Optional[str]:
        """Use Python PDF library to keep bottom portion"""
        try:
            pdf_lib = None
            try:
                import PyPDF2 as pdf_lib
                pdf_class = pdf_lib.PdfFileReader
                pdf_writer_class = pdf_lib.PdfFileWriter
            except ImportError:
                try:
                    import PyPDF4 as pdf_lib
                    pdf_class = pdf_lib.PdfFileReader
                    pdf_writer_class = pdf_lib.PdfFileWriter
                except ImportError:
                    try:
                        import pypdf as pdf_lib
                        pdf_class = pdf_lib.PdfReader
                        pdf_writer_class = pdf_lib.PdfWriter
                    except ImportError:
                        return None
            
            if not pdf_lib:
                return None
            
            crop_height_percent = crop_settings.get('keep_bottom_percent', 80)
            
            async with aiofiles.open(pdf_path, 'rb') as input_file:
                content = await input_file.read()
            
            from io import BytesIO
            pdf_reader = pdf_class(BytesIO(content))
            pdf_writer = pdf_writer_class()
            
            page_count = len(pdf_reader.pages) if hasattr(pdf_reader, 'pages') else pdf_reader.getNumPages()
            
            for page_num in range(page_count):
                if hasattr(pdf_reader, 'pages'):
                    page = pdf_reader.pages[page_num]
                else:
                    page = pdf_reader.getPage(page_num)
                
                if hasattr(page, 'mediabox'):
                    mediabox = page.mediabox
                elif hasattr(page, 'mediaBox'):
                    mediabox = page.mediaBox
                else:
                    continue
                
                width = float(mediabox[2] - mediabox[0])
                height = float(mediabox[3] - mediabox[1])
                
                new_height = height * (crop_height_percent / 100.0)
                
                crop_box = [
                    mediabox[0],
                    mediabox[1],
                    mediabox[2],
                    mediabox[1] + new_height
                ]
                
                if hasattr(page, 'cropbox'):
                    page.cropbox = crop_box
                elif hasattr(page, 'cropBox'):
                    page.cropBox = crop_box
                
                if hasattr(pdf_writer, 'add_page'):
                    pdf_writer.add_page(page)
                else:
                    pdf_writer.addPage(page)
            
            output_file = tempfile.NamedTemporaryFile(
                suffix='_cropped.pdf', 
                delete=False,
                prefix="cropped_bottom_"
            )
            
            pdf_writer.write(output_file)
            output_file.close()
            
            if os.path.exists(output_file.name) and os.path.getsize(output_file.name) > 0:
                if self.logger.isEnabledFor(logging.INFO):
                    self.logger.info(f"Python bottom-crop successful (keep {crop_height_percent}%)")
                return output_file.name
            else:
                self.logger.error("Python bottom-crop produced empty file")
                self._cleanup_temp_file(output_file.name)
                return None
                    
        except Exception as e:
            self.logger.error(f"Python bottom-crop error: {e}")
            return None
    
    async def _crop_custom(self, pdf_path: str, crop_settings: Dict[str, Any]) -> Optional[str]:
        """Apply custom crop box settings"""
        try:
            crop_box = crop_settings.get('crop_box', [0, 0, 612, 792])
            return await self._crop_custom_python(pdf_path, crop_box)
                
        except Exception as e:
            self.logger.error(f"Custom cropping failed: {e}")
            return None
    
    async def _crop_custom_python(self, pdf_path: str, crop_box: List[float]) -> Optional[str]:
        """Apply custom crop box using Python PDF library"""
        try:
            import PyPDF2 as pdf_lib
            
            if len(crop_box) != 4:
                self.logger.error("Custom crop box must have 4 values: [left, bottom, right, top]")
                return None
            
            crop_left, crop_bottom, crop_right, crop_top = crop_box
            
            async with aiofiles.open(pdf_path, 'rb') as input_file:
                content = await input_file.read()
            
            from io import BytesIO
            pdf_reader = pdf_lib.PdfReader(BytesIO(content))
            pdf_writer = pdf_lib.PdfWriter()
            
            for page in pdf_reader.pages:
                mediabox = page.mediabox
                
                original_left = float(mediabox.left)
                original_bottom = float(mediabox.bottom)
                original_right = float(mediabox.right)
                original_top = float(mediabox.top)
                
                abs_left = original_left + crop_left
                abs_bottom = original_bottom + crop_bottom
                abs_right = original_left + crop_right
                abs_top = original_bottom + crop_top
                
                abs_left = max(abs_left, original_left)
                abs_bottom = max(abs_bottom, original_bottom)
                abs_right = min(abs_right, original_right)
                abs_top = min(abs_top, original_top)
                
                if abs_left >= abs_right or abs_bottom >= abs_top:
                    self.logger.error(f"Invalid crop box")
                    continue
                
                page.cropbox.lower_left = (abs_left, abs_bottom)
                page.cropbox.upper_right = (abs_right, abs_top)
                
                pdf_writer.add_page(page)
            
            output_file = tempfile.NamedTemporaryFile(
                suffix='_cropped.pdf', 
                delete=False,
                prefix="cropped_custom_"
            )
            
            pdf_writer.write(output_file)
            output_file.close()
            
            if os.path.exists(output_file.name) and os.path.getsize(output_file.name) > 0:
                if self.logger.isEnabledFor(logging.INFO):
                    self.logger.info(f"Python custom-crop successful")
                return output_file.name
            else:
                self.logger.error("Python custom-crop produced empty file")
                self._cleanup_temp_file(output_file.name)
                return None
                    
        except Exception as e:
            self.logger.error(f"Python custom-crop error: {e}")
            return None
    
    def _validate_print_job(self, job: Dict[str, Any]) -> bool:
        """Validate print job parameters"""
        printer_name = job.get('printer_name')
        if not printer_name:
            self.logger.error("No printer name specified in job")
            return False
        
        if not self.printer_manager.is_printer_available(printer_name):
            self.logger.error(f"Printer '{printer_name}' is not available")
            return False
        
        content_type = job.get('content_type', '').lower()
        if content_type not in ['pdf', 'base64_pdf', 'url']:
            self.logger.error(f"Unsupported content type: {content_type}")
            return False
        
        if content_type == 'base64_pdf' and not job.get('content'):
            self.logger.error("No base64 content provided")
            return False
        
        if content_type in ['pdf', 'url'] and not job.get('content_url'):
            self.logger.error("No content URL provided")
            return False
        
        return True
    
    async def _prepare_pdf_content(self, job: Dict[str, Any]) -> Optional[str]:
        """Prepare PDF content from various sources"""
        content_type = job.get('content_type', '').lower()
        
        try:
            if content_type == 'base64_pdf':
                return await self._handle_base64_content(job)
            elif content_type in ['pdf', 'url']:
                return await self._handle_url_content(job)
            else:
                self.logger.error(f"Unknown content type: {content_type}")
                return None
                
        except Exception as e:
            self.logger.error(f"Content preparation failed: {e}")
            return None
    
    async def _handle_base64_content(self, job: Dict[str, Any]) -> Optional[str]:
        """Handle base64 PDF content"""
        try:
            content = job.get('content', '')
            if not content:
                self.logger.error("Empty base64 content")
                return None
            
            pdf_data = base64.b64decode(content)
            
            temp_file = tempfile.NamedTemporaryFile(
                suffix='.pdf', 
                delete=False,
                prefix=f"printjob_{job.get('id', 'unknown')}_"
            )
            
            async with aiofiles.open(temp_file.name, 'wb') as f:
                await f.write(pdf_data)
            
            temp_file.close()
            
            self.logger.debug(f"Created PDF from base64 content: {temp_file.name}")
            return temp_file.name
            
        except Exception as e:
            self.logger.error(f"Base64 content handling failed: {e}")
            return None
    
    async def _handle_url_content(self, job: Dict[str, Any]) -> Optional[str]:
        """Handle URL-based PDF content"""
        try:
            url = job.get('content_url', '')
            if not url:
                self.logger.error("Empty content URL")
                return None
            
            self.logger.debug(f"Downloading PDF from: {url}")
            
            timeout = aiohttp.ClientTimeout(total=15, connect=5)
            headers = {'User-Agent': 'WindowsPrintService/1.0'}
            
            connector = aiohttp.TCPConnector(limit=10)
            
            async with aiohttp.ClientSession(timeout=timeout, headers=headers, connector=connector) as session:
                async with session.get(url) as response:
                    if response.status != 200:
                        self.logger.error(f"Download failed with status {response.status}")
                        return None
                    
                    content_type = response.headers.get('content-type', '').lower()
                    if 'pdf' not in content_type:
                        self.logger.warning(f"Unexpected content type: {content_type}")
                    
                    temp_file = tempfile.NamedTemporaryFile(
                        suffix='.pdf',
                        delete=False,
                        prefix=f"printjob_{job.get('id', 'unknown')}_"
                    )
                    temp_file.close()
                    
                    async with aiofiles.open(temp_file.name, 'wb') as f:
                        async for chunk in response.content.iter_chunked(8192):
                            await f.write(chunk)
                    
                    self.logger.debug(f"Downloaded PDF to: {temp_file.name}")
                    return temp_file.name
            
        except asyncio.TimeoutError:
            self.logger.error("PDF download timeout")
            return None
        except Exception as e:
            self.logger.error(f"URL content handling failed: {e}")
            return None
    
    async def _execute_print(self, pdf_path: str, job: Dict[str, Any]) -> bool:
        """Execute the actual printing using the best available tool"""
        printer_name = job['printer_name']
        settings = job.get('settings', {})
        
        if self.logger.isEnabledFor(logging.DEBUG):
            self.logger.debug(f"Starting print execution: {pdf_path} -> {printer_name}")
        
        scaling_mode = settings.get('scaling', 'auto_scale').lower()
        force_orientation = settings.get('force_orientation', False)
        rotation = settings.get('rotation', 0)
        
        if force_orientation or rotation != 0:
            preprocessed_pdf = None
            
            if rotation != 0:
                preprocessed_pdf = await self._rotate_pdf_python(pdf_path, rotation)
                if not preprocessed_pdf and self.ghostscript_path:
                    preprocessed_pdf = await self._preprocess_pdf_orientation(pdf_path, settings)
            elif force_orientation and self.ghostscript_path:
                preprocessed_pdf = await self._preprocess_pdf_orientation(pdf_path, settings)
            
            if preprocessed_pdf:
                try:
                    simple_settings = settings.copy()
                    simple_settings.pop('rotation', None)
                    simple_settings.pop('force_orientation', None)
                    
                    if self.preferred_tool == "sumatra":
                        success = await self._print_with_sumatra_simple(preprocessed_pdf, printer_name, simple_settings)
                    elif self.preferred_tool == "adobe":
                        success = await self._print_with_adobe(preprocessed_pdf, printer_name, simple_settings)
                    else:
                        success = await self._print_with_system_default(preprocessed_pdf, printer_name)
                    
                    return success
                finally:
                    self._cleanup_temp_file(preprocessed_pdf)
        
        if scaling_mode == 'fit_to_paper' and self.ghostscript_path:
            success = await self._print_with_ghostscript_fit_to_paper(pdf_path, printer_name, settings)
            if success:
                return True
            else:
                fallback_settings = settings.copy()
                fallback_settings['scaling'] = 'fit_to_paper_enhanced'
                return await self._print_with_sumatra_simple(pdf_path, printer_name, fallback_settings)
        elif scaling_mode == 'fit_to_paper':
            enhanced_settings = settings.copy()
            enhanced_settings['scaling'] = 'fit_to_paper_enhanced'
            return await self._print_with_sumatra_simple(pdf_path, printer_name, enhanced_settings)
        
        if self.preferred_tool == "sumatra":
            return await self._print_with_sumatra_simple(pdf_path, printer_name, settings)
        elif self.preferred_tool == "adobe":
            return await self._print_with_adobe(pdf_path, printer_name, settings)
        else:
            return await self._print_with_system_default(pdf_path, printer_name)
    
    async def _rotate_pdf_python(self, pdf_path: str, rotation: int) -> Optional[str]:
        """Rotate PDF using Python PDF library"""
        try:
            rotation = rotation % 360
            if rotation == 0:
                return None
            
            pdf_lib = None
            try:
                import PyPDF2 as pdf_lib
                pdf_class = pdf_lib.PdfReader
                pdf_writer_class = pdf_lib.PdfWriter
            except (ImportError, AttributeError):
                try:
                    import PyPDF4 as pdf_lib
                    pdf_class = pdf_lib.PdfFileReader
                    pdf_writer_class = pdf_lib.PdfFileWriter
                except ImportError:
                    try:
                        import pypdf as pdf_lib
                        pdf_class = pdf_lib.PdfReader
                        pdf_writer_class = pdf_lib.PdfWriter
                    except ImportError:
                        return None
            
            if not pdf_lib:
                return None
            
            async with aiofiles.open(pdf_path, 'rb') as input_file:
                content = await input_file.read()
            
            from io import BytesIO
            pdf_reader = pdf_class(BytesIO(content))
            pdf_writer = pdf_writer_class()
            
            page_count = len(pdf_reader.pages) if hasattr(pdf_reader, 'pages') else pdf_reader.getNumPages()
            
            for page_num in range(page_count):
                if hasattr(pdf_reader, 'pages'):
                    page = pdf_reader.pages[page_num]
                    rotated_page = page.rotate(rotation)
                    pdf_writer.add_page(rotated_page)
                else:
                    page = pdf_reader.getPage(page_num)
                    if rotation == 90:
                        rotated_page = page.rotateClockwise(90)
                    elif rotation == 180:
                        rotated_page = page.rotateClockwise(90).rotateClockwise(90)
                    elif rotation == 270:
                        rotated_page = page.rotateCounterClockwise(90)
                    else:
                        rotated_page = page
                    pdf_writer.addPage(rotated_page)
            
            output_file = tempfile.NamedTemporaryFile(
                suffix='_rotated.pdf', 
                delete=False,
                prefix="rotated_"
            )
            
            pdf_writer.write(output_file)
            output_file.close()
            
            if os.path.exists(output_file.name) and os.path.getsize(output_file.name) > 0:
                return output_file.name
            else:
                self._cleanup_temp_file(output_file.name)
                return None
                    
        except Exception as e:
            self.logger.error(f"Python PDF rotation error: {e}")
            return None
    
    async def _preprocess_pdf_orientation(self, pdf_path: str, settings: Dict[str, Any]) -> Optional[str]:
        """Preprocess PDF to fix orientation and rotation using Ghostscript"""
        try:
            output_file = tempfile.NamedTemporaryFile(
                suffix='_oriented.pdf', 
                delete=False,
                prefix="oriented_"
            )
            output_file.close()
            
            orientation = settings.get('orientation', 'portrait').lower()
            rotation = settings.get('rotation', 0)
            paper_size = settings.get('paper_size', 'letter').lower()
            
            paper_sizes = {
                'letter': (612, 792),
                'a4': (595, 842), 
                'legal': (612, 1008),
                'a3': (842, 1191),
                'tabloid': (792, 1224)
            }
            
            width, height = paper_sizes.get(paper_size, (612, 792))
            
            target_width, target_height = width, height
            if orientation == 'landscape':
                target_width, target_height = height, width
            
            cmd = [
                self.ghostscript_path,
                "-dNOPAUSE",
                "-dBATCH",
                "-sDEVICE=pdfwrite",
                "-dCompatibilityLevel=1.4",
                "-dPDFSETTINGS=/printer",
                f"-dDEVICEWIDTHPOINTS={target_width}",
                f"-dDEVICEHEIGHTPOINTS={target_height}",
                "-dFIXEDMEDIA",
                "-dAutoRotatePages=/None",
            ]
            
            postscript_commands = []
            
            if rotation and rotation != 0:
                rotation = rotation % 360
                if rotation == 90:
                    postscript_commands.append(f"gsave 0 {target_width} translate 90 rotate")
                elif rotation == 180:
                    postscript_commands.append(f"gsave {target_width} {target_height} translate 180 rotate")
                elif rotation == 270:
                    postscript_commands.append(f"gsave {target_height} 0 translate 270 rotate")
            
            postscript_commands.append(f"<</PageSize [{target_width} {target_height}] /Orientation 0>> setpagedevice")
            
            if rotation and rotation != 0:
                postscript_commands.append("grestore")
            
            if postscript_commands:
                cmd.extend(["-c", " ".join(postscript_commands)])
            
            cmd.extend([pdf_path, f"-sOutputFile={output_file.name}"])
            
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            
            stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=10)
            
            if process.returncode == 0 and os.path.exists(output_file.name):
                return output_file.name
            else:
                self._cleanup_temp_file(output_file.name)
                return None
                    
        except Exception as e:
            self.logger.error(f"PDF orientation preprocessing error: {e}")
            return None
    
    async def _print_with_ghostscript_fit_to_paper(self, pdf_path: str, printer_name: str, settings: Dict[str, Any]) -> bool:
        """Use Ghostscript to properly fit PDF to paper size without cropping"""
        try:
            output_file = tempfile.NamedTemporaryFile(
                suffix='_fitted.pdf', 
                delete=False,
                prefix="fitted_"
            )
            output_file.close()
            
            paper_size = settings.get('paper_size', 'letter').lower()
            orientation = settings.get('orientation', 'portrait').lower()
            rotation = settings.get('rotation', 0)
            force_orientation = settings.get('force_orientation', False)
            
            paper_sizes = {
                'letter': (612, 792),
                'a4': (595, 842), 
                'legal': (612, 1008),
                'a3': (842, 1191),
                'tabloid': (792, 1224)
            }
            
            width, height = paper_sizes.get(paper_size, (612, 792))
            
            if orientation == 'landscape':
                width, height = height, width
            
            cmd = [
                self.ghostscript_path,
                "-dNOPAUSE",
                "-dBATCH",
                "-sDEVICE=pdfwrite",
                "-dCompatibilityLevel=1.4",
                "-dPDFSETTINGS=/printer",
                f"-dDEVICEWIDTHPOINTS={width}",
                f"-dDEVICEHEIGHTPOINTS={height}",
                "-dFIXEDMEDIA",
                "-dPDFFitPage",
            ]
            
            if force_orientation:
                cmd.append("-dAutoRotatePages=/None")
                
                if rotation and rotation != 0:
                    rotation = rotation % 360
                    if rotation != 0:
                        cmd.extend(["-c", f"<</Orientation {rotation//90}>> setpagedevice"])
            else:
                if orientation == 'portrait':
                    cmd.append("-dAutoRotatePages=/None")
                else:
                    cmd.append("-dAutoRotatePages=/PageByPage")
            
            cmd.append("-dUseCropBox")
            cmd.extend([f"-sOutputFile={output_file.name}", pdf_path])
            
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            
            stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=10)
            
            if process.returncode == 0 and os.path.exists(output_file.name):
                fitted_settings = settings.copy()
                fitted_settings['scaling'] = 'no_scale'
                fitted_settings.pop('auto_scale', None)
                fitted_settings['paper_size'] = settings.get('paper_size', 'letter')
                fitted_settings['force_orientation'] = True
                
                if self.preferred_tool == "sumatra":
                    success = await self._print_with_sumatra_simple(output_file.name, printer_name, fitted_settings)
                elif self.preferred_tool == "adobe":
                    success = await self._print_with_adobe(output_file.name, printer_name, fitted_settings)
                else:
                    success = await self._print_with_system_default(output_file.name, printer_name)
                
                self._cleanup_temp_file(output_file.name)
                return success
                
            else:
                self._cleanup_temp_file(output_file.name)
                return False
                
        except Exception as e:
            self.logger.error(f"Ghostscript fit-to-paper error: {e}")
            return False
    
    async def _print_with_sumatra_simple(self, pdf_path: str, printer_name: str, settings: Dict[str, Any]) -> bool:
        """Print using SumatraPDF"""
        try:
            cmd = [self.sumatra_path, "-print-to", printer_name, "-silent"]
            
            print_settings = []
            
            copies = settings.get('copies', 1)
            if copies > 1:
                print_settings.append(f"copies={copies}")
            
            orientation = settings.get('orientation', '').lower()
            scaling_mode = settings.get('scaling', 'auto_scale').lower()
            
            if orientation == 'landscape':
                print_settings.append("orientation=landscape")
            elif orientation == 'portrait':
                print_settings.append("orientation=portrait")
            
            if scaling_mode in ['fit_to_paper', 'fit_to_paper_enhanced']:
                print_settings.append("duplex=off")
            
            if scaling_mode == 'fit_to_paper_enhanced':
                print_settings.append("scale=noscale")
                print_settings.append("autorotate=yes")
                print_settings.append("center=yes")
            elif scaling_mode == 'fit_to_paper':
                print_settings.append("scale=noscale")
                print_settings.append("autorotate=yes")
            elif scaling_mode == 'fit_to_paper_force':
                print_settings.append("scale=fit")
                print_settings.append("autorotate=yes")
            elif scaling_mode == 'shrink_to_fit':
                print_settings.append("scale=shrink")
            elif scaling_mode == 'actual_size' or scaling_mode == 'none':
                print_settings.append("scale=none")
            elif scaling_mode == 'no_scale':
                print_settings.append("scale=noscale")
            elif scaling_mode.startswith('custom_'):
                try:
                    scale_percent = int(scaling_mode.split('_')[1])
                    if 10 <= scale_percent <= 500:
                        print_settings.append(f"scale={scale_percent}%")
                    else:
                        print_settings.append("scale=noscale")
                except (ValueError, IndexError):
                    print_settings.append("scale=noscale")
            elif settings.get('auto_scale', True):
                print_settings.append("scale=shrink")
            
            if print_settings:
                cmd.extend(["-print-settings", ",".join(print_settings)])
            
            cmd.append(pdf_path)
            
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                creationflags=subprocess.CREATE_NO_WINDOW if os.name == 'nt' else 0
            )
            
            try:
                stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=15)
                return process.returncode == 0
                    
            except asyncio.TimeoutError:
                process.terminate()
                try:
                    await asyncio.wait_for(process.wait(), timeout=5)
                except asyncio.TimeoutError:
                    process.kill()
                return False
                
        except Exception as e:
            self.logger.error(f"SumatraPDF execution error: {e}")
            return False
    
    async def _print_with_adobe(self, pdf_path: str, printer_name: str, settings: Dict[str, Any]) -> bool:
        """Print using Adobe Reader"""
        try:
            cmd = [self.adobe_path, "/t", pdf_path, printer_name]
            
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                creationflags=subprocess.CREATE_NO_WINDOW if os.name == 'nt' else 0
            )
            
            try:
                stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=15)
                return process.returncode == 0
                
            except asyncio.TimeoutError:
                process.terminate()
                return False
                
        except Exception as e:
            self.logger.error(f"Adobe Reader execution error: {e}")
            return False
    
    async def _print_with_system_default(self, pdf_path: str, printer_name: str) -> bool:
        """Print using Windows system default handler"""
        try:
            if os.name == 'nt':
                cmd = ["cmd", "/c", "start", "/min", "", pdf_path]
                
                result = subprocess.run(
                    cmd,
                    capture_output=True,
                    timeout=10,
                    creationflags=subprocess.CREATE_NO_WINDOW
                )
                
                return result.returncode == 0
            else:
                return False
                
        except Exception as e:
            self.logger.error(f"System default print error: {e}")
            return False
    
    def _cleanup_temp_file(self, file_path: str):
        """Clean up temporary files safely"""
        try:
            if file_path and os.path.exists(file_path):
                os.unlink(file_path)
        except Exception as e:
            self.logger.warning(f"Failed to cleanup {file_path}: {e}")
    
    async def _extract_pages(self, pdf_path: str, page_range: str) -> Optional[str]:
        """Extract specific pages from PDF"""
        try:
            if self.pdftk_path:
                return await self._extract_pages_pdftk(pdf_path, page_range)
            elif self.ghostscript_path:
                return await self._extract_pages_ghostscript(pdf_path, page_range)
            else:
                return await self._extract_pages_python(pdf_path, page_range)
        except Exception as e:
            self.logger.error(f"Page extraction failed: {e}")
            return None
    
    async def _extract_pages_pdftk(self, pdf_path: str, page_range: str) -> Optional[str]:
        """Extract pages using PDFtk"""
        try:
            output_file = tempfile.NamedTemporaryFile(
                suffix='_extracted.pdf', 
                delete=False,
                prefix="extracted_"
            )
            output_file.close()
            
            cmd = [self.pdftk_path, pdf_path, "cat", page_range, "output", output_file.name]
            
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            
            stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=10)
            
            if process.returncode == 0 and os.path.exists(output_file.name):
                return output_file.name
            else:
                self._cleanup_temp_file(output_file.name)
                return None
                
        except Exception as e:
            self.logger.error(f"PDFtk extraction error: {e}")
            return None
    
    async def _extract_pages_ghostscript(self, pdf_path: str, page_range: str) -> Optional[str]:
        """Extract pages using Ghostscript"""
        try:
            output_file = tempfile.NamedTemporaryFile(
                suffix='_extracted.pdf', 
                delete=False,
                prefix="extracted_"
            )
            output_file.close()
            
            if page_range == "1":
                gs_range = "1-1"
            elif "-" in page_range:
                gs_range = page_range
            else:
                gs_range = f"{page_range}-{page_range}"
            
            cmd = [
                self.ghostscript_path,
                "-dNOPAUSE",
                "-dBATCH",
                "-sDEVICE=pdfwrite",
                f"-dFirstPage={gs_range.split('-')[0]}",
                f"-dLastPage={gs_range.split('-')[-1]}",
                f"-sOutputFile={output_file.name}",
                pdf_path
            ]
            
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            
            stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=10)
            
            if process.returncode == 0 and os.path.exists(output_file.name):
                return output_file.name
            else:
                self._cleanup_temp_file(output_file.name)
                return None
                
        except Exception as e:
            self.logger.error(f"Ghostscript extraction error: {e}")
            return None
    
    async def _extract_pages_python(self, pdf_path: str, page_range: str) -> Optional[str]:
        """Extract pages using Python"""
        try:
            pdf_lib = None
            try:
                import PyPDF2 as pdf_lib
                pdf_class = pdf_lib.PdfFileReader
                pdf_writer_class = pdf_lib.PdfFileWriter
            except ImportError:
                try:
                    import PyPDF4 as pdf_lib
                    pdf_class = pdf_lib.PdfFileReader
                    pdf_writer_class = pdf_lib.PdfFileWriter
                except ImportError:
                    try:
                        import pypdf as pdf_lib
                        pdf_class = pdf_lib.PdfReader
                        pdf_writer_class = pdf_lib.PdfWriter
                    except ImportError:
                        return None
            
            if not pdf_lib:
                return None
            
            if page_range == "1":
                pages_to_extract = [0]
            elif "-" in page_range:
                start, end = page_range.split("-", 1)
                pages_to_extract = list(range(int(start) - 1, int(end)))
            else:
                pages_to_extract = [int(page_range) - 1]
            
            async with aiofiles.open(pdf_path, 'rb') as input_file:
                content = await input_file.read()
            
            from io import BytesIO
            pdf_reader = pdf_class(BytesIO(content))
            pdf_writer = pdf_writer_class()
            
            for page_num in pages_to_extract:
                if page_num < len(pdf_reader.pages if hasattr(pdf_reader, 'pages') else pdf_reader.getNumPages()):
                    if hasattr(pdf_reader, 'pages'):
                        pdf_writer.add_page(pdf_reader.pages[page_num])
                    else:
                        pdf_writer.addPage(pdf_reader.getPage(page_num))
            
            output_file = tempfile.NamedTemporaryFile(
                suffix='_extracted.pdf', 
                delete=False,
                prefix="extracted_"
            )
            
            pdf_writer.write(output_file)
            output_file.close()
            
            if os.path.exists(output_file.name) and os.path.getsize(output_file.name) > 0:
                return output_file.name
            else:
                self._cleanup_temp_file(output_file.name)
                return None
                    
        except Exception as e:
            self.logger.error(f"Python PDF extraction error: {e}")
            return None
    
    async def _get_pdf_page_count(self, pdf_path: str) -> Optional[int]:
        """Get the number of pages in a PDF"""
        try:
            if self.pdftk_path:
                return await self._get_page_count_pdftk(pdf_path)
            
            if self.ghostscript_path:
                return await self._get_page_count_ghostscript(pdf_path)
            
            return await self._get_page_count_python(pdf_path)
            
        except Exception as e:
            self.logger.error(f"Failed to get PDF page count: {e}")
            return None

    async def _get_page_count_pdftk(self, pdf_path: str) -> Optional[int]:
        """Get page count using PDFtk"""
        try:
            cmd = [self.pdftk_path, pdf_path, "dump_data"]
            
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            
            stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=5)
            
            if process.returncode == 0:
                output = stdout.decode()
                for line in output.split('\n'):
                    if line.startswith('NumberOfPages:'):
                        return int(line.split(':')[1].strip())
            
            return None
            
        except Exception as e:
            return None

    async def _get_page_count_ghostscript(self, pdf_path: str) -> Optional[int]:
        """Get page count using Ghostscript"""
        try:
            cmd = [
                self.ghostscript_path,
                "-dNODISPLAY",
                "-dBATCH",
                "-dNOPAUSE",
                "-c",
                "pdfpagecount",
                "-f",
                pdf_path
            ]
            
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            
            stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=5)
            
            if process.returncode == 0:
                output = stdout.decode().strip()
                try:
                    return int(output.split()[-1])
                except (ValueError, IndexError):
                    return None
            
            return None
            
        except Exception as e:
            return None

    async def _get_page_count_python(self, pdf_path: str) -> Optional[int]:
        """Get page count using Python PDF library"""
        try:
            try:
                import PyPDF2 as pdf_lib
                pdf_class = pdf_lib.PdfFileReader
            except ImportError:
                try:
                    import PyPDF4 as pdf_lib
                    pdf_class = pdf_lib.PdfFileReader
                except ImportError:
                    try:
                        import pypdf as pdf_lib
                        pdf_class = pdf_lib.PdfReader
                    except ImportError:
                        return None
            
            async with aiofiles.open(pdf_path, 'rb') as file:
                content = await file.read()
            
            from io import BytesIO
            pdf_reader = pdf_class(BytesIO(content))
            if hasattr(pdf_reader, 'pages'):
                return len(pdf_reader.pages)
            else:
                return pdf_reader.getNumPages()
            
        except Exception as e:
            return None
    
    async def execute_multi_printer_job(self, pdf_url: str, printer_assignments: List[Dict[str, Any]]) -> bool:
        """Execute a job across multiple printers concurrently"""
        try:
            if self.logger.isEnabledFor(logging.INFO):
                self.logger.info(f"Executing multi-printer job with {len(printer_assignments)} assignments")
            
            temp_job = {
                "id": f"multi_{int(time.time())}",
                "content_type": "pdf",
                "content_url": pdf_url
            }
            
            pdf_path = await self._prepare_pdf_content(temp_job)
            if not pdf_path:
                self.logger.error("Failed to prepare PDF for multi-printer job")
                return False
            
            try:
                # Execute all printer assignments concurrently
                tasks = []
                for assignment in printer_assignments:
                    task = self._print_pages_to_printer(
                        pdf_path=pdf_path,
                        printer_name=assignment['printer_name'],
                        page_range=assignment.get('pages', 'all'),
                        settings=assignment.get('settings', {})
                    )
                    tasks.append(task)
                
                results = await asyncio.gather(*tasks, return_exceptions=True)
                
                successful = sum(1 for result in results if result is True)
                total = len(results)
                
                if self.logger.isEnabledFor(logging.INFO):
                    self.logger.info(f"Multi-printer job completed: {successful}/{total} successful")
                
                return successful == total
                
            finally:
                self._cleanup_temp_file(pdf_path)
                
        except Exception as e:
            self.logger.error(f"Multi-printer job execution failed: {e}")
            return False
    
    async def _print_pages_to_printer(self, pdf_path: str, printer_name: str, page_range: str, settings: Dict[str, Any]) -> bool:
        """Print specific pages to a specific printer"""
        try:
            job_settings = settings.copy()
            if page_range and page_range != 'all':
                job_settings['page_range'] = page_range
            
            job = {
                'printer_name': printer_name,
                'settings': job_settings
            }
            
            return await self.execute_print_job({
                **job,
                'content_type': 'pdf',
                'content_url': f'file://{pdf_path}',
                'id': f'multi_page_{int(time.time())}'
            })
            
        except Exception as e:
            self.logger.error(f"Page-specific print failed for {printer_name}: {e}")
            return False
    
    def get_performance_stats(self) -> Dict[str, Any]:
        """Get performance statistics"""
        avg_time = (self.total_processing_time / self.jobs_processed) if self.jobs_processed > 0 else 0
        success_rate = (self.successful_jobs / self.jobs_processed * 100) if self.jobs_processed > 0 else 0
        
        return {
            "jobs_processed": self.jobs_processed,
            "successful_jobs": self.successful_jobs,
            "failed_jobs": self.jobs_processed - self.successful_jobs,
            "success_rate_percent": round(success_rate, 1),
            "average_processing_time_ms": round(avg_time * 1000, 1),
            "total_processing_time_seconds": round(self.total_processing_time, 2),
            "preferred_tool": self.preferred_tool,
            "concurrent_print_limit": PrintExecutor._print_semaphore._value
        }
    
    def _get_page_extraction_method(self) -> str:
        """Get the preferred page extraction method"""
        if self.pdftk_path:
            return "pdftk"
        elif self.ghostscript_path:
            return "ghostscript"
        else:
            return "python_builtin"
    
    def _python_pdf_available(self) -> bool:
        """Check if Python PDF libraries are available"""
        try:
            import PyPDF2
            return True
        except ImportError:
            try:
                import PyPDF4
                return True
            except ImportError:
                try:
                    import pypdf
                    return True
                except ImportError:
                    return False
    
    def get_tool_info(self) -> Dict[str, Any]:
        """Get information about available PDF tools"""
        return {
            "preferred_tool": self.preferred_tool,
            "concurrent_execution": "Enabled (max 10 concurrent prints)",
            "batch_processing": "Supported",
            "available_tools": {
                "sumatra": self.sumatra_path is not None,
                "adobe": self.adobe_path is not None,
                "pdftk": self.pdftk_path is not None,
                "ghostscript": self.ghostscript_path is not None
            }
        }