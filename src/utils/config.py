"""
Configuration manager untuk distributed system.
File ini membaca environment variables dan menyediakan
konfigurasi default untuk semua komponen sistem.
"""

import os
from typing import List
from dotenv import load_dotenv

# Load environment variables dari .env file
load_dotenv()


class Config:
    """Class untuk manage semua konfigurasi sistem"""
    
    # Node Configuration
    NODE_ID: int = int(os.getenv('NODE_ID', 1))
    NODE_HOST: str = os.getenv('NODE_HOST', 'localhost')
    NODE_PORT: int = int(os.getenv('NODE_PORT', 5000))
    
    # Peer Configuration
    @staticmethod
    def get_peers() -> List[str]:
        """
        Parse peer nodes dari environment variable.
        Format: "host1:port1,host2:port2"
        Returns: List of peer addresses
        """
        peers_str = os.getenv('PEERS', '')
        if not peers_str:
            return []
        return [peer.strip() for peer in peers_str.split(',')]
    
    # Redis Configuration
    REDIS_HOST: str = os.getenv('REDIS_HOST', 'localhost')
    REDIS_PORT: int = int(os.getenv('REDIS_PORT', 6379))
    REDIS_DB: int = int(os.getenv('REDIS_DB', 0))
    
    # Raft Configuration (dalam milliseconds)
    ELECTION_TIMEOUT_MIN: int = int(os.getenv('ELECTION_TIMEOUT_MIN', 150))
    ELECTION_TIMEOUT_MAX: int = int(os.getenv('ELECTION_TIMEOUT_MAX', 300))
    HEARTBEAT_INTERVAL: int = int(os.getenv('HEARTBEAT_INTERVAL', 50))
    
    # Cache Configuration
    CACHE_SIZE: int = int(os.getenv('CACHE_SIZE', 1000))
    CACHE_POLICY: str = os.getenv('CACHE_POLICY', 'LRU')
    
    # Logging
    LOG_LEVEL: str = os.getenv('LOG_LEVEL', 'INFO')
    LOG_FILE: str = os.getenv('LOG_FILE', 'logs/node.log')
    
    @classmethod
    def display(cls):
        """Print semua konfigurasi untuk debugging"""
        print("=== Configuration ===")
        print(f"Node ID: {cls.NODE_ID}")
        print(f"Node Address: {cls.NODE_HOST}:{cls.NODE_PORT}")
        print(f"Peers: {cls.get_peers()}")
        print(f"Redis: {cls.REDIS_HOST}:{cls.REDIS_PORT}")
        print("=" * 30)


# Test configuration saat file dijalankan langsung
if __name__ == "__main__":
    Config.display()
