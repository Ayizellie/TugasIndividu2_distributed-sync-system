"""Communication package initialization"""

from .message_passing import MessagePassing, Message, MessageType
from .failure_detector import FailureDetector

__all__ = ['MessagePassing', 'Message', 'MessageType', 'FailureDetector']
