"""
Script untuk run single lock manager node
"""
import asyncio
import sys
import logging
from src.nodes.lock_manager import LockManager
from src.utils.config import Config

async def main():
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Create node
    node = LockManager(
        node_id=Config.NODE_ID,
        host=Config.NODE_HOST,
        port=Config.NODE_PORT,
        peers=Config.get_peers()
    )
    
    # Start node
    await node.start()
    
    print(f"\n{'='*60}")
    print(f"  LOCK MANAGER NODE {Config.NODE_ID} RUNNING")
    print(f"  Address: http://{Config.NODE_HOST}:{Config.NODE_PORT}")
    print(f"  Peers: {Config.get_peers()}")
    print(f"{'='*60}\n")
    print("Press Ctrl+C to stop...\n")
    
    try:
        # Keep running
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        print("\n\nStopping node...")
        await node.stop()
        print("Node stopped.")

if __name__ == "__main__":
    asyncio.run(main())
