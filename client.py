#!/usr/bin/env python3

import asyncio
import time
import logging
import uuid
import grpc
from grpc import aio

import large_message_pb2
import large_message_pb2_grpc

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class AggressiveBlockingTest:
    def __init__(self, server_address='localhost:50051'):
        self.server_address = server_address
        self.channel = None
        self.stub = None
    
    async def connect(self):
        self.channel = aio.insecure_channel(
            self.server_address,
            options=[
                ('grpc.max_send_message_length', 1048 * 1024 * 1024),
                ('grpc.max_receive_message_length', 1048 * 1024 * 1024),
            ]
        )
        self.stub = large_message_pb2_grpc.LargeMessageServiceStub(self.channel)
        logger.info(f"Connected to server at {self.server_address}")
    
    async def disconnect(self):
        if self.channel:
            await self.channel.close()
    
    async def send_health_check(self):
        try:
            request = large_message_pb2.HealthRequest(service_name="LargeMessageService")
            start_time = time.time()
            response = await self.stub.HealthCheck(request)
            end_time = time.time()
            duration = end_time - start_time
            
            return response, duration
            
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            raise
    
    async def send_large_message(self, size_mb=64):
        data_size = size_mb * 1024 * 1024
        data = b'x' * data_size
        
        message_id = str(uuid.uuid4())
        timestamp = int(time.time())
        
        request = large_message_pb2.LargeMessage(
            data=data,
            message_id=message_id,
            timestamp=timestamp
        )
        
        logger.info(f"Sending {size_mb}MB message...")
        start_time = time.time()
        
        try:
            response = await self.stub.ProcessLargeMessage(request)
            end_time = time.time()
            duration = end_time - start_time
            
            logger.info(f"{size_mb}MB message completed in {duration:.2f}s")
            return response
            
        except Exception as e:
            logger.error(f"{size_mb}MB message failed: {e}")
            raise
    
    async def test_rapid_health_checks(self):
        logger.info("=== Testing Rapid Health Checks ===")
        
        # Start large message processing
        logger.info("1. Starting large message processing...")
        large_message_task = asyncio.create_task(self.send_large_message(1000))
        
        # Wait a moment for processing to start
        # await asyncio.sleep(0.5)
        
        # Send health checks very rapidly
        logger.info("2. Sending rapid health checks...")
        
        health_check_times = []
        for i in range(20):  # More frequent checks
            try:
                response, duration = await self.send_health_check()
                health_check_times.append(duration)
                
                if duration > 0.1:
                    logger.info(f"   ⚠️  Health check {i+1}: {duration:.3f}s (BLOCKED)")
                else:
                    logger.info(f"   ✅ Health check {i+1}: {duration:.3f}s (normal)")
                    
            except Exception as e:
                logger.error(f"   ❌ Health check {i+1} failed: {e}")
                health_check_times.append(None)
            
            # Very short delay between checks
            await asyncio.sleep(0.1)
        
        # Wait for large message to complete
        logger.info("3. Waiting for large message to complete...")
        try:
            large_message_result = await large_message_task
            logger.info("✅ Large message processing completed")
        except Exception as e:
            logger.error(f"❌ Large message failed: {e}")
        
        # Analysis
        blocked_checks = sum(1 for d in health_check_times if d is not None and d > 0.1)
        failed_checks = sum(1 for d in health_check_times if d is None)
        
        logger.info("")
        logger.info("=== Results ===")
        logger.info(f"Total health checks: {len(health_check_times)}")
        logger.info(f"Blocked checks (>0.1s): {blocked_checks}")
        logger.info(f"Failed checks: {failed_checks}")
        logger.info(f"Normal checks: {len(health_check_times) - blocked_checks - failed_checks}")
        
        if failed_checks > 0:
            logger.info("❌ SERVER WAS UNRESPONSIVE: Some health checks failed")
        elif blocked_checks > 0:
            logger.info("⚠️  SERVER WAS PARTIALLY BLOCKED: Some health checks took longer")
        else:
            logger.info("✅ SERVER REMAINED RESPONSIVE: All health checks completed quickly")
    
async def main():
    """Main function."""
    test = AggressiveBlockingTest()
    
    try:
        await test.connect()
        await test.test_rapid_health_checks()
        
    except Exception as e:
        logger.error(f"Test error: {e}")
    finally:
        await test.disconnect()

if __name__ == '__main__':
    asyncio.run(main())
