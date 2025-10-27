"""
Message passing layer untuk inter-node communication.
Menggunakan aiohttp untuk async HTTP communication.
"""

import asyncio
import aiohttp
import json
from typing import Dict, Any, Optional, List
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class MessageType(Enum):
    """Tipe-tipe message dalam sistem"""
    # Raft messages
    REQUEST_VOTE = "request_vote"
    VOTE_RESPONSE = "vote_response"
    APPEND_ENTRIES = "append_entries"
    APPEND_RESPONSE = "append_response"
    
    # Lock messages
    ACQUIRE_LOCK = "acquire_lock"
    RELEASE_LOCK = "release_lock"
    LOCK_RESPONSE = "lock_response"
    
    # Queue messages
    ENQUEUE = "enqueue"
    DEQUEUE = "dequeue"
    QUEUE_RESPONSE = "queue_response"
    
    # Cache messages
    CACHE_GET = "cache_get"
    CACHE_PUT = "cache_put"
    CACHE_INVALIDATE = "cache_invalidate"
    CACHE_RESPONSE = "cache_response"
    
    # Heartbeat
    HEARTBEAT = "heartbeat"
    HEARTBEAT_RESPONSE = "heartbeat_response"


class Message:
    """
    Class untuk represent message yang dikirim antar nodes.
    """
    
    def __init__(self, 
                 msg_type: MessageType,
                 sender_id: int,
                 term: int = 0,
                 data: Optional[Dict[str, Any]] = None):
        """
        Args:
            msg_type: Tipe message
            sender_id: ID node pengirim
            term: Current term (untuk Raft)
            data: Payload data
        """
        self.msg_type = msg_type
        self.sender_id = sender_id
        self.term = term
        self.data = data or {}
        self.timestamp = asyncio.get_event_loop().time()
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert message ke dictionary untuk JSON serialization"""
        return {
            'msg_type': self.msg_type.value,
            'sender_id': self.sender_id,
            'term': self.term,
            'data': self.data,
            'timestamp': self.timestamp
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Message':
        """Create message dari dictionary"""
        return cls(
            msg_type=MessageType(data['msg_type']),
            sender_id=data['sender_id'],
            term=data.get('term', 0),
            data=data.get('data', {})
        )
    
    def __repr__(self):
        return f"Message({self.msg_type.value}, from={self.sender_id}, term={self.term})"


class MessagePassing:
    """
    Class untuk handle sending dan receiving messages.
    Menggunakan aiohttp untuk async HTTP communication.
    """
    
    def __init__(self, node_id: int, host: str, port: int):
        """
        Args:
            node_id: ID node ini
            host: Host address
            port: Port number
        """
        self.node_id = node_id
        self.host = host
        self.port = port
        self.session: Optional[aiohttp.ClientSession] = None
        
        # Statistics
        self.messages_sent = 0
        self.messages_received = 0
        self.failed_sends = 0
    
    async def initialize(self):
        """Initialize HTTP client session"""
        self.session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=5)  
        )
        logger.info(f"MessagePassing initialized for node {self.node_id}")
    
    async def close(self):
        """Close HTTP client session"""
        if self.session:
            await self.session.close()
        logger.info(f"MessagePassing closed for node {self.node_id}")
    
    async def send_message(self, 
                          target_address: str,
                          message: Message) -> Optional[Dict[str, Any]]:
        """
        Send message ke target node.
        
        Args:
            target_address: Format "host:port"
            message: Message object to send
            
        Returns:
            Response dari target node, atau None jika gagal
        """
        if not self.session:
            await self.initialize()
        
        url = f"http://{target_address}/api/message"
        
        try:
            async with self.session.post(url, json=message.to_dict()) as response:
                if response.status == 200:
                    self.messages_sent += 1
                    result = await response.json()
                    logger.debug(f"Sent {message.msg_type.value} to {target_address}")
                    return result
                else:
                    logger.warning(f"Failed to send message to {target_address}: {response.status}")
                    self.failed_sends += 1
                    return None
                    
        except asyncio.TimeoutError:
            logger.warning(f"Timeout sending message to {target_address}")
            self.failed_sends += 1
            return None
            
        except Exception as e:
            logger.error(f"Error sending message to {target_address}: {e}")
            self.failed_sends += 1
            return None
    
    async def broadcast_message(self,
                               peer_addresses: List[str],
                               message: Message) -> List[Optional[Dict[str, Any]]]:
        """
        Broadcast message ke multiple nodes secara parallel.
        
        Args:
            peer_addresses: List of "host:port" addresses
            message: Message to broadcast
            
        Returns:
            List of responses (None untuk failed sends)
        """
        # Gunakan asyncio.gather untuk parallel sending
        tasks = [
            self.send_message(address, message)
            for address in peer_addresses
        ]
        
        # Wait for all tasks to complete
        responses = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Convert exceptions to None
        responses = [
            r if not isinstance(r, Exception) else None
            for r in responses
        ]
        
        logger.info(f"Broadcast {message.msg_type.value} to {len(peer_addresses)} peers, "
                   f"{sum(1 for r in responses if r is not None)} successful")
        
        return responses
    
    def get_stats(self) -> Dict[str, int]:
        """Get message passing statistics"""
        return {
            'messages_sent': self.messages_sent,
            'messages_received': self.messages_received,
            'failed_sends': self.failed_sends
        }


# Test code
async def test_message_passing():
    """Test message passing functionality"""
    mp = MessagePassing(node_id=1, host='localhost', port=5000)
    await mp.initialize()
    
    # Create test message
    msg = Message(
        msg_type=MessageType.HEARTBEAT,
        sender_id=1,
        term=1,
        data={'status': 'alive'}
    )
    
    print(f"Message: {msg}")
    print(f"Message dict: {msg.to_dict()}")
    
    # Test serialization
    msg2 = Message.from_dict(msg.to_dict())
    print(f"Deserialized: {msg2}")
    
    await mp.close()


if __name__ == "__main__":
    # Setup logging
    logging.basicConfig(level=logging.DEBUG)
    
    # Run test
    asyncio.run(test_message_passing())
