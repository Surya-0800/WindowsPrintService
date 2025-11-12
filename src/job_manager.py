"""
Job Manager
Ultra-fast job polling and processing with adaptive intervals and retry limits
"""

import asyncio
import aiohttp
import logging
import time
import json
from typing import Dict, Any, Optional

class JobManager:
    """Manages job polling and processing with ultra-fast response times and retry limits"""
    
    def __init__(self, config: Dict[str, Any], print_executor, printer_manager):
        self.config = config
        self.print_executor = print_executor
        self.printer_manager = printer_manager
        self.logger = logging.getLogger(__name__)
        
        # State management
        self.running = False
        self.session: Optional[aiohttp.ClientSession] = None
        
        # NEW: Job retry tracking
        self.job_retry_counts = {}  # job_id -> retry_count
        self.max_job_retries = 3   # Maximum retries per job
        
        # Performance tracking
        self.total_polls = 0
        self.jobs_processed = 0
        self.jobs_failed_permanently = 0  # NEW: Track permanently failed jobs
        self.last_successful_contact = 0
        self.consecutive_errors = 0
        
        # Polling configuration
        self.poll_interval = config.get('poll_interval', 0.1)  # 100ms default
        self.max_retries = config.get('max_retries', 3)
        self.timeout_seconds = config.get('timeout_seconds', 30)
    
    async def start_processing(self):
        """Start the ultra-fast job processing loop"""
        self.running = True
        
        # Create optimized HTTP session
        connector = aiohttp.TCPConnector(
            limit=20,
            ttl_dns_cache=300,
            use_dns_cache=True,
            keepalive_timeout=60
        )
        
        timeout = aiohttp.ClientTimeout(
            total=self.timeout_seconds,
            connect=5,
            sock_read=10
        )
        
        self.session = aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
            headers={'User-Agent': 'WindowsPrintService/1.0'}
        )
        
        try:
            self.logger.info("Starting ultra-fast job processing")
            self.logger.info(f"Poll interval: {self.poll_interval*1000:.0f}ms")
            self.logger.info(f"Max retries per job: {self.max_job_retries}")
            
            while self.running:
                try:
                    # Process pending jobs
                    await self._process_pending_jobs()
                    
                    # Clean up old retry counts periodically
                    if self.total_polls % 1000 == 0:  # Every 1000 polls (~100 seconds)
                        self._cleanup_old_retry_counts()
                    
                    # Update metrics
                    self.total_polls += 1
                    
                    # Sleep for poll interval
                    await asyncio.sleep(self.poll_interval)
                    
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    self.consecutive_errors += 1
                    self.logger.error(f"Job processing error #{self.consecutive_errors}: {e}")
                    
                    # Exponential backoff on errors (max 5 seconds)
                    error_sleep = min(5.0, 0.1 * (2 ** min(self.consecutive_errors, 5)))
                    await asyncio.sleep(error_sleep)
                    
        finally:
            if self.session:
                await self.session.close()
            self.logger.info("Job processing stopped")
    
    async def _process_pending_jobs(self):
        """Check for and process pending jobs"""
        try:
            # Build request URL and parameters
            url = f"{self.config['server_url']}/api/print-jobs/pending/"
            params = {"client_id": self.config['client_id']}
            headers = {}
            
            # Add API key if configured
            if self.config.get('api_key'):
                headers['X-API-Key'] = self.config['api_key']
            
            # Make the request
            async with self.session.get(url, params=params, headers=headers) as response:
                if response.status == 200:
                    try:
                        # Get response content type
                        content_type = response.headers.get('content-type', '').lower()
                        
                        if 'application/json' in content_type:
                            jobs = await response.json()
                        else:
                            # Not JSON - log the response for debugging
                            text_response = await response.text()
                            self.logger.warning(f"Server returned non-JSON response (content-type: {content_type}): {text_response[:200]}...")
                            return  # Skip processing
                        
                        # Validate that jobs is a list or dict, not a string
                        if isinstance(jobs, str):
                            self.logger.error(f"Server returned string instead of JSON object: {jobs[:200]}...")
                            return
                        
                        # Handle different response formats
                        if isinstance(jobs, dict):
                            # If response is wrapped in an object (e.g., {"pending_jobs": [...], "success": true})
                            if 'pending_jobs' in jobs:
                                jobs = jobs['pending_jobs']
                            elif 'jobs' in jobs:
                                jobs = jobs['jobs']
                            elif 'data' in jobs:
                                jobs = jobs['data']
                            elif jobs.get('success') is True:
                                # Status response with no jobs - check for pending_jobs field
                                jobs = jobs.get('pending_jobs', [])
                            else:
                                # Single job object - wrap in list
                                jobs = [jobs] if 'id' in jobs else []
                        
                        # Ensure jobs is a list
                        if not isinstance(jobs, list):
                            self.logger.error(f"Expected jobs to be a list, got {type(jobs)}: {jobs}")
                            return
                        
                        if jobs and len(jobs) > 0:
                            self.logger.info(f"Raw jobs from server: {jobs}")
                            
                            # Validate job objects
                            valid_jobs = []
                            for job in jobs:
                                if isinstance(job, dict) and 'id' in job:
                                    valid_jobs.append(job)
                                else:
                                    self.logger.warning(f"Invalid job object (missing 'id'): {job}")
                            
                            if valid_jobs:
                                self.logger.info(f"Valid jobs found: {len(valid_jobs)}")
                                
                                # Filter out jobs that have exceeded retry limit
                                processable_jobs = self._filter_processable_jobs(valid_jobs)
                                
                                if processable_jobs:
                                    self.logger.info(f"Processing {len(processable_jobs)} jobs (filtered from {len(valid_jobs)} pending)")
                                    
                                    # Process jobs concurrently for speed
                                    max_concurrent = self.config.get('max_concurrent_jobs', 5)
                                    semaphore = asyncio.Semaphore(max_concurrent)
                                    
                                    tasks = [
                                        self._process_single_job_with_semaphore(semaphore, job)
                                        for job in processable_jobs
                                    ]
                                    
                                    await asyncio.gather(*tasks, return_exceptions=True)
                                else:
                                    self.logger.debug(f"All {len(valid_jobs)} jobs have exceeded retry limit, skipping")
                            else:
                                self.logger.warning(f"No valid job objects found in response")
                            
                        # Reset error counter on successful contact
                        self.consecutive_errors = 0
                        self.last_successful_contact = time.time()
                        
                    except json.JSONDecodeError as e:
                        # Get the raw response for debugging
                        try:
                            raw_text = await response.text()
                            self.logger.error(f"Invalid JSON response: {e}. Raw response: {raw_text[:500]}...")
                        except:
                            self.logger.error(f"Invalid JSON response: {e}")
                        
                elif response.status == 404:
                    # Endpoint not found - server might not be ready
                    self.logger.debug("Pending jobs endpoint not found")
                    
                elif response.status == 500:
                    # Server error - log response for debugging
                    try:
                        error_text = await response.text()
                        self.logger.error(f"Server error (500): {error_text[:300]}...")
                    except:
                        self.logger.error("Server error (500) - could not read response")
                        
                else:
                    try:
                        error_text = await response.text()
                        self.logger.warning(f"Server returned status {response.status}: {error_text[:200]}...")
                    except:
                        self.logger.warning(f"Server returned status {response.status}")
                    
        except asyncio.TimeoutError:
            self.logger.warning("Request timeout while checking for jobs")
            
        except aiohttp.ClientError as e:
            self.logger.warning(f"Network error: {e}")
            
        except Exception as e:
            import traceback
            self.logger.error(f"Unexpected error in job polling: {e}")
            self.logger.debug(f"Full traceback: {traceback.format_exc()}")
    
    def _filter_processable_jobs(self, jobs: list) -> list:
        """Filter out jobs that have exceeded retry limits"""
        processable_jobs = []
        
        for job in jobs:
            job_id = job.get('id', 'unknown')
            current_retry_count = self.job_retry_counts.get(job_id, 0)
            
            if current_retry_count < self.max_job_retries:
                processable_jobs.append(job)
            else:
                # Job has exceeded retry limit - mark as permanently failed
                if current_retry_count == self.max_job_retries:
                    self.logger.warning(f"Job {job_id} exceeded {self.max_job_retries} retries, marking as permanently failed")
                    asyncio.create_task(self._mark_job_permanently_failed(job_id))
                    # Increment to prevent repeated logging
                    self.job_retry_counts[job_id] = current_retry_count + 1
                    self.jobs_failed_permanently += 1
        
        return processable_jobs
    
    async def _mark_job_permanently_failed(self, job_id: str):
        """Mark a job as permanently failed after max retries"""
        error_message = f"Job failed permanently after {self.max_job_retries} retry attempts"
        await self._update_job_status(job_id, "failed", error_message)
        self.logger.error(f"Job {job_id}: {error_message}")
    
    async def _process_single_job_with_semaphore(self, semaphore: asyncio.Semaphore, job: Dict[str, Any]):
        """Process a single job with concurrency control"""
        async with semaphore:
            await self._process_single_job(job)
    
    async def _process_single_job(self, job: Dict[str, Any]):
        """Process a single print job with retry tracking"""
        job_id = job.get('id', 'unknown')
        start_time = time.time()
        
        # Track retry attempt
        current_retry = self.job_retry_counts.get(job_id, 0)
        self.job_retry_counts[job_id] = current_retry + 1
        
        try:
            retry_info = f" (attempt {current_retry + 1}/{self.max_job_retries})" if current_retry > 0 else ""
            self.logger.info(f"Processing job {job_id}{retry_info}")
            
            # Update job status to processing
            await self._update_job_status(job_id, "processing")
            
            # Execute the print job
            success = await self.print_executor.execute_print_job(job)
            
            if success:
                # Job succeeded - remove from retry tracking and mark completed
                self.job_retry_counts.pop(job_id, None)
                await self._update_job_status(job_id, "completed")
                
                # Log completion with timing
                processing_time = (time.time() - start_time) * 1000
                success_info = f" (succeeded after {current_retry + 1} attempts)" if current_retry > 0 else ""
                self.logger.info(f"Job {job_id} completed in {processing_time:.0f}ms{success_info}")
                
                # Update metrics
                self.jobs_processed += 1
                
            else:
                # Job failed - check if we should retry
                if self.job_retry_counts[job_id] >= self.max_job_retries:
                    # Max retries reached - mark as permanently failed
                    error_message = f"Job failed after {self.max_job_retries} attempts"
                    await self._update_job_status(job_id, "failed", error_message)
                    self.jobs_failed_permanently += 1
                    
                    processing_time = (time.time() - start_time) * 1000
                    self.logger.error(f"Job {job_id} permanently failed after {self.max_job_retries} attempts ({processing_time:.0f}ms)")
                else:
                    # Will retry - don't update status yet, just log
                    processing_time = (time.time() - start_time) * 1000
                    next_attempt = self.job_retry_counts[job_id] + 1
                    self.logger.warning(f"Job {job_id} failed (attempt {self.job_retry_counts[job_id]}/{self.max_job_retries}) - will retry as attempt {next_attempt} ({processing_time:.0f}ms)")
            
        except Exception as e:
            # Exception during processing - still counts as a retry attempt
            processing_time = (time.time() - start_time) * 1000
            
            if self.job_retry_counts[job_id] >= self.max_job_retries:
                # Max retries reached
                error_message = f"Job failed with exception after {self.max_job_retries} attempts: {str(e)}"
                await self._update_job_status(job_id, "failed", error_message)
                self.jobs_failed_permanently += 1
                self.logger.error(f"Job {job_id} permanently failed with exception after {self.max_job_retries} attempts: {e} ({processing_time:.0f}ms)")
            else:
                # Will retry
                next_attempt = self.job_retry_counts[job_id] + 1
                self.logger.error(f"Job {job_id} failed with exception (attempt {self.job_retry_counts[job_id]}/{self.max_job_retries}): {e} - will retry as attempt {next_attempt} ({processing_time:.0f}ms)")
    
    def _cleanup_old_retry_counts(self):
        """Clean up retry counts for jobs that are no longer in the system"""
        # This is a simple cleanup - in a production system you might want to
        # track job timestamps and clean up based on age
        if len(self.job_retry_counts) > 1000:  # Arbitrary limit
            # Keep only the most recent 500 entries
            items = list(self.job_retry_counts.items())
            self.job_retry_counts = dict(items[-500:])
            self.logger.debug(f"Cleaned up old retry counts, keeping {len(self.job_retry_counts)} entries")
    
    async def _update_job_status(self, job_id: str, status: str, error: Optional[str] = None):
        """Update job status on the server with retry logic"""
        for attempt in range(self.max_retries):
            try:
                url = f"{self.config['server_url']}/api/print-jobs/status/"
                data = {
                    "job_id": job_id,
                    "status": status,
                    "error": error,
                    "client_id": self.config['client_id'],
                    "timestamp": time.time()
                }
                
                # NEW: Add retry information for failed jobs
                if status == "failed" and job_id in self.job_retry_counts:
                    data["retry_count"] = self.job_retry_counts[job_id]
                    data["max_retries"] = self.max_job_retries
                
                headers = {'Content-Type': 'application/json'}
                if self.config.get('api_key'):
                    headers['X-API-Key'] = self.config['api_key']
                
                async with self.session.post(url, json=data, headers=headers) as response:
                    if response.status in [200, 201]:
                        self.logger.debug(f"Updated job {job_id} status to {status}")
                        return
                    else:
                        self.logger.warning(f"Status update failed with status {response.status}")
                        
            except Exception as e:
                if attempt == self.max_retries - 1:
                    self.logger.error(f"Failed to update job status after {self.max_retries} attempts: {e}")
                else:
                    # Progressive delay between retries
                    await asyncio.sleep(0.5 * (attempt + 1))
    
    # Burst mode for ultra-fast response
    async def trigger_burst_mode(self, duration: float = 5.0):
        """Trigger burst mode for ultra-fast polling"""
        try:
            self.logger.info(f"Activating burst mode for {duration} seconds")
            
            # Save current poll interval
            original_interval = self.poll_interval
            
            # Set burst interval
            burst_interval = self.config.get('burst_interval', 0.05)  # 50ms
            self.poll_interval = burst_interval
            
            # Wait for burst duration
            await asyncio.sleep(duration)
            
            # Restore original interval
            self.poll_interval = original_interval
            
            self.logger.info("Burst mode deactivated")
            
        except Exception as e:
            self.logger.error(f"Burst mode error: {e}")
    
    # NEW: Method to manually reset retry count for a job (if needed)
    def reset_job_retry_count(self, job_id: str):
        """Reset retry count for a specific job"""
        if job_id in self.job_retry_counts:
            old_count = self.job_retry_counts.pop(job_id)
            self.logger.info(f"Reset retry count for job {job_id} (was {old_count})")
            return True
        return False
    
    # Status and health monitoring
    def is_healthy(self) -> bool:
        """Check if job manager is healthy"""
        current_time = time.time()
        
        # Consider unhealthy if no contact with server for 2 minutes
        if current_time - self.last_successful_contact > 120:
            return False
        
        # Consider unhealthy if too many consecutive errors
        if self.consecutive_errors > 10:
            return False
        
        return True
    
    def get_status(self) -> Dict[str, Any]:
        """Get comprehensive job manager status"""
        current_time = time.time()
        
        return {
            "running": self.running,
            "poll_interval_ms": self.poll_interval * 1000,
            "total_polls": self.total_polls,
            "jobs_processed": self.jobs_processed,
            "jobs_failed_permanently": self.jobs_failed_permanently,  # NEW
            "jobs_being_retried": len(self.job_retry_counts),  # NEW
            "max_job_retries": self.max_job_retries,  # NEW
            "last_successful_contact": self.last_successful_contact,
            "seconds_since_contact": current_time - self.last_successful_contact,
            "consecutive_errors": self.consecutive_errors,
            "healthy": self.is_healthy(),
            "server_url": self.config['server_url'],
            "client_id": self.config['client_id']
        }
    
    def get_performance_metrics(self) -> Dict[str, Any]:
        """Get performance metrics"""
        uptime = time.time() - self.last_successful_contact if self.last_successful_contact > 0 else 0
        polls_per_second = self.total_polls / uptime if uptime > 0 else 0
        
        # NEW: Calculate success rate including permanent failures
        total_job_attempts = self.jobs_processed + self.jobs_failed_permanently
        success_rate = (self.jobs_processed / total_job_attempts * 100) if total_job_attempts > 0 else 100
        
        return {
            "total_polls": self.total_polls,
            "jobs_processed": self.jobs_processed,
            "jobs_failed_permanently": self.jobs_failed_permanently,  # NEW
            "jobs_being_retried": len(self.job_retry_counts),  # NEW
            "job_success_rate_percent": round(success_rate, 1),  # NEW
            "polls_per_second": round(polls_per_second, 2),
            "average_response_time_ms": round(self.poll_interval * 1000, 1),
            "consecutive_errors": self.consecutive_errors,
            "error_rate": (self.consecutive_errors / max(self.total_polls, 1)) * 100
        }
    
    def stop_processing(self):
        """Stop job processing"""
        self.running = False
        self.logger.info("Job processing stop requested")