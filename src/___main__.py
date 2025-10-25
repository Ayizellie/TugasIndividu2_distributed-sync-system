"""
Main entry point untuk menjalankan distributed system.
"""

import asyncio
import argparse
import logging
import sys

from src.nodes.lock_manager import LockManager
from src.nodes.queue_node import QueueNode
from src.nodes.cache_node import CacheNode
from src.utils.config import Config


def setup_logging():
    """Setup logging configuration"""
    logging.basicConfig(
        level=getattr(logging, Config.LOG_LEVEL),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(Config.LOG_FILE) if Config.LOG_FILE else logging.NullHandler()
        ]
    )


async def run_node(node_type: str):
    """
    Run specific node type.
    
    Args:
        node_type: Type of node to run (lock, queue, cache)
    """
    # Get configuration
    node_id = Config.NODE_ID
    host = Config.NODE_HOST
    peers = Config.get_peers()
    
    # Create node based on type
    if node_type == 'lock':
        port = Config.NODE_PORT
        node = LockManager(node_id, host, port, peers)
    elif node_type == 'queue':
        port = Config.NODE_PORT
        node = QueueNode(node_id, host, port, peers)
    elif node_type == 'cache':
        port = Config.NODE_PORT
        node = CacheNode(node_id, host, port, peers, cache_size=Config.CACHE_SIZE)
    else:
        print(f"Unknown node type: {node_type}")
        return
    
    # Start node
    await node.start()
    
    print(f"\n{'='*60}")
    print(f"  {node_type.upper()} NODE {node_id} STARTED")
    print(f"  Address: http://{host}:{port}")
    print(f"  Peers: {peers}")
    print(f"{'='*60}\n")
    
    try:
        # Keep running
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        print("\nShutting down...")
    finally:
        await node.stop()


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description='Distributed Synchronization System')
    parser.add_argument(
        'node_type',
        choices=['lock', 'queue', 'cache'],
        help='Type of node to run'
    )
    parser.add_argument(
        '--config',
        help='Path to config file',
        default=None
    )
    
    args = parser.parse_args()
    
    # Setup logging
    setup_logging()
    
    # Display configuration
    Config.display()
    
    # Run node
    try:
        asyncio.run(run_node(args.node_type))
    except KeyboardInterrupt:
        print("\nExiting...")
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
