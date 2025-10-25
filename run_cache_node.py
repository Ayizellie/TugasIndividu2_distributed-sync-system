"""
Script untuk run single cache node
"""
import asyncio
import logging
from src.nodes.cache_node import CacheNode
from src.utils.config import Config

async def main():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    node = CacheNode(
        node_id=Config.NODE_ID,
        host=Config.NODE_HOST,
        port=Config.NODE_PORT,
        peers=Config.get_peers(),
        cache_size=Config.CACHE_SIZE
    )
    
    await node.start()
    
    print(f"\n{'='*60}")
    print(f"  CACHE NODE {Config.NODE_ID} RUNNING")
    print(f"  Address: http://{Config.NODE_HOST}:{Config.NODE_PORT}")
    print(f"  Peers: {Config.get_peers()}")
    print(f"{'='*60}\n")
    print("Press Ctrl+C to stop...\n")
    
    try:
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        print("\n\nStopping node...")
        await node.stop()
        print("Node stopped.")

if __name__ == "__main__":
    asyncio.run(main())
