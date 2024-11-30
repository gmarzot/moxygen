import asyncio
import argparse
import ssl
import socket
from pathlib import Path
from urllib.parse import urlparse
from aioquic.quic.configuration import QuicConfiguration
from aioquic.quic.connection import QuicConnection
from aioquic.quic.events import QuicEvent, StreamDataReceived, ConnectionTerminated
from typing import Dict, Optional, Tuple

class QuicTestClient:
    def __init__(self, host: str, port: int, configuration: QuicConfiguration):
        self.host = host
        self.port = port
        self.configuration = configuration
        self.connection: Optional[QuicConnection] = None
        self.control_stream_id: Optional[int] = None
        self.streams: Dict[int, bytes] = {}
        self.transport = None
        self.protocol = None

    async def connect(self):
        try:
            # Create connection
            self.connection = QuicConnection(
                configuration=self.configuration,
            )
            
            # Establish connection
            await self._establish_connection()
            
            # Open control stream
            self.control_stream_id = self.connection.get_next_available_stream_id()
            print(f"Opening control stream {self.control_stream_id}")
            
            # Start event loop
            await self._handle_events()
        except Exception as e:
            print(f"Connection initialization failed: {e}")
            raise

    async def _establish_connection(self):
        try:
            # Resolve hostname first
            addr_info = await asyncio.get_event_loop().getaddrinfo(
                self.host, 
                self.port,
                family=socket.AF_INET,
                type=socket.SOCK_DGRAM
            )
            if not addr_info:
                raise ConnectionError(f"Could not resolve {self.host}")
            
            # Create the protocol
            self.protocol = QuicProtocol(self.connection)
            
            # Create transport
            self.transport, _ = await asyncio.get_event_loop().create_datagram_endpoint(
                lambda: self.protocol,
                remote_addr=addr_info[0][4]  # Use the first resolved address
            )
            
            # Connect
            print(f"Attempting connection to {self.host}:{self.port}")
            await self.connection.connect(self.host, self.port, now=asyncio.get_event_loop().time())
            print(f"Connected to {self.host}:{self.port}")
            
        except Exception as e:
            if self.transport:
                self.transport.close()
            print(f"Connection establishment failed: {str(e)}")
            raise

    async def send_control_message(self, message: bytes):
        if self.control_stream_id is not None and self.connection:
            self.connection.send_stream_data(self.control_stream_id, message)
            print(f"Sent control message: {message}")
            # Ensure the data is actually sent
            self.connection.datagrams_to_send(now=asyncio.get_event_loop().time())

    async def create_unidirectional_stream(self) -> int:
        if not self.connection:
            raise ConnectionError("No active connection")
        stream_id = self.connection.get_next_available_stream_id(is_unidirectional=True)
        print(f"Created unidirectional stream {stream_id}")
        return stream_id

    async def _handle_events(self):
        try:
            while True:
                event = self.connection.next_event()
                if event is None:
                    # Process any pending datagrams
                    datagrams = self.connection.datagrams_to_send(now=asyncio.get_event_loop().time())
                    for datagram, _ in datagrams:
                        self.transport.sendto(datagram, (self.host, self.port))
                    await asyncio.sleep(0.1)
                    continue

                if isinstance(event, StreamDataReceived):
                    self._handle_stream_data(event)
                elif isinstance(event, ConnectionTerminated):
                    print("Connection terminated")
                    break
        except Exception as e:
            print(f"Event handling error: {e}")
            raise

    def _handle_stream_data(self, event: StreamDataReceived):
        stream_id = event.stream_id
        if stream_id not in self.streams:
            self.streams[stream_id] = bytes()
        self.streams[stream_id] += event.data
        print(f"Received data on stream {stream_id}: {event.data}")

class QuicProtocol:
    def __init__(self, quic: QuicConnection):
        self.quic = quic
        self.transport = None

    def connection_made(self, transport):
        self.transport = transport

    def datagram_received(self, data: bytes, addr):
        try:
            self.quic.receive_datagram(data, addr, now=asyncio.get_event_loop().time())
        except Exception as e:
            print(f"Error processing received datagram: {e}")

    def connection_lost(self, exc):
        print(f"UDP connection lost: {exc}")

def setup_quic_configuration(cert_path: Path, key_path: Path) -> QuicConfiguration:
    """Create QUIC configuration with SSL context."""
    configuration = QuicConfiguration(
        is_client=True,
        alpn_protocols=["moq-00"],  # Changed to MOQ protocol
        verify_mode=ssl.CERT_NONE,  # Skip certificate verification for testing
        idle_timeout=5.0,  # Add explicit timeout
        max_datagram_size=1350,  # Typical MTU size
    )
    
    if cert_path and key_path:
        try:
            ssl_context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
            ssl_context.load_cert_chain(cert_path, key_path)
            configuration.ssl_context = ssl_context
            print(f"Loaded certificates from {cert_path} and {key_path}")
        except Exception as e:
            print(f"Error loading certificates: {e}")
            raise
    
    return configuration

def parse_url(url: str) -> Tuple[str, int]:
    """Parse URL into host and port."""
    if "://" not in url:
        url = "quic://" + url
    parsed = urlparse(url)
    host = parsed.hostname or "localhost"
    port = parsed.port or 4433
    return host, port

async def main():
    parser = argparse.ArgumentParser(description="QUIC Test Client")
    parser.add_argument(
        "--url", 
        default="localhost:4433",
        help="URL to connect to (default: localhost:4433)"
    )
    parser.add_argument(
        "--cert", 
        type=Path,
        help="Path to certificate file (PEM format)"
    )
    parser.add_argument(
        "--key", 
        type=Path,
        help="Path to private key file"
    )
    args = parser.parse_args()

    if bool(args.cert) != bool(args.key):
        parser.error("Both --cert and --key must be provided together")

    try:
        # Parse URL and create configuration
        host, port = parse_url(args.url)
        print(f"Connecting to {host}:{port}")
        
        configuration = setup_quic_configuration(args.cert, args.key)
        
        # Create and run test client
        client = QuicTestClient(host, port, configuration)
        await client.connect()

        # Send a test control message
        test_message = b"Hello, MOQ relay!"
        await client.send_control_message(test_message)

        # Create a unidirectional stream
        stream_id = await client.create_unidirectional_stream()
        
        # Keep the connection alive for a while
        await asyncio.sleep(5)
    
    except KeyboardInterrupt:
        print("\nShutting down...")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if hasattr(client, 'connection') and client.connection:
            client.connection.close()
        if hasattr(client, 'transport') and client.transport:
            client.transport.close()

if __name__ == "__main__":
    asyncio.run(main())