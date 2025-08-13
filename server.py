#!/usr/bin/env python3
import asyncio
import time
import logging
import os
from concurrent.futures import ThreadPoolExecutor
import grpc
from grpc import aio

import large_message_pb2
import large_message_pb2_grpc

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class LargeMessageServicer(large_message_pb2_grpc.LargeMessageServiceServicer):
    def __init__(self):
        self.executor = ThreadPoolExecutor(max_workers=4)
        self.request_count = 0
    
    async def ProcessLargeMessage(self, request, context):
        start_time = time.time()
        self.request_count += 1
        request_id = self.request_count
        
        logger.info(f"Processing large message {request_id}, size: {len(request.data)} bytes")
        
        # Simulate processing time that can block the event loop
        # This is where the blocking occurs according to the issue
        processing_time = await self._process_large_data(request.data)
        
        end_time = time.time()
        total_time_ms = int((end_time - start_time) * 1000)
        
        logger.info(f"Completed processing message {request_id} in {total_time_ms}ms")
        
        return large_message_pb2.LargeMessageResponse(
            status="SUCCESS",
            message_id=request.message_id,
            processing_time_ms=total_time_ms,
            data_size=len(request.data)
        )
    
    async def HealthCheck(self, request, context):
        return large_message_pb2.HealthResponse(
            status="HEALTHY",
            timestamp=time.strftime("%Y-%m-%d %H:%M:%S")
        )
    
    async def _process_large_data(self, data):
        data_size = len(data)
        
        logger.info(f"Starting to process {data_size} bytes of data...")
        
        processing_time = min(data_size / (1024 * 1024), 5.0)  # Up to 5 seconds for large data
        
        logger.info(f"Processing will take {processing_time:.1f} seconds (blocking event loop)")
        
        await asyncio.sleep(processing_time)
        
        logger.info("Performing CPU-intensive work on large data...")
        checksum = sum(data[i] for i in range(0, min(len(data), 1000), 100))
        
        logger.info(f"Completed processing {data_size} bytes")
        
        return processing_time

async def serve():
    server = aio.server(
        ThreadPoolExecutor(max_workers=10),
        options=[
            ('grpc.max_send_message_length', 1048 * 1024 * 1024),  # 100MB
            ('grpc.max_receive_message_length', 1048 * 1024 * 1024),  # 100MB
        ]
    )
    
    large_message_pb2_grpc.add_LargeMessageServiceServicer_to_server(
        LargeMessageServicer(), server
    )
    
    listen_addr = '[::]:50051'
    server.add_insecure_port(listen_addr)
    
    logger.info(f"Starting gRPC server on {listen_addr}")
    
    await server.start()
    logger.info("Server started successfully")
    
    try:
        await server.wait_for_termination()
    except KeyboardInterrupt:
        logger.info("Shutting down server...")
        await server.stop(grace=5)

if __name__ == '__main__':
    asyncio.run(serve())
