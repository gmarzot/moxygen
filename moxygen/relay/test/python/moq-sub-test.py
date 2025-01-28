#!/usr/bin/env python3

import argparse
import logging
import json
import sys
import ssl

import asyncio
import aioquic
from aioquic.buffer import Buffer, UINT_VAR_MAX
from aioquic.quic.logger import QuicLogger
from aioquic.asyncio.client import connect
from aioquic.asyncio.protocol import QuicConnectionProtocol
from aioquic.quic.configuration import QuicConfiguration
from aioquic.quic.events import QuicEvent, StreamDataReceived
from aioquic.h3.connection import H3_ALPN, H3Connection
from aioquic.h3.events import (
    DataReceived,
    HeadersReceived,
    WebTransportStreamDataReceived,
)

from typing import Optional
from collections import defaultdict

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

USER_AGENT = "aioquic/" + aioquic.__version__

class MoqtClientProtocol(QuicConnectionProtocol):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._h3: Optional[H3Connection] = None
        self._control_stream_id = None # MOQT control stream
        self._streams = {}  # MOQT streams
        self._wt_session_setup= asyncio.Event()  # WebTransport session
        self._moqt_session_setup = asyncio.Event()  # CLIENT_SETUP/SERVER_SETUP exchange
        self._groups = defaultdict(lambda: {'objects': 0, 'subgroups': set()})

    def connection_made(self, transport):
        super().connection_made(transport)
        self._h3 = H3Connection(self._quic, enable_webtransport=True)
        logger.info("H3 connection initialized")
 
    def quic_event_received(self, event: QuicEvent) -> None:
        if hasattr(event, 'error_code'):
            logger.error(f"QUIC EVENT: error: {event.error_code} reason: {getattr(event, 'reason_phrase', 'unknown')}")
            return
    
        logger.info(f"QUIC EVENT: stream {getattr(event, 'stream_id', 'unknown')}")

        if hasattr(event, 'data'):
            logger.debug(f"QUIC EVENT: stream {getattr(event, 'stream_id', 'unknown')} data: 0x{event.data.hex()}")
        
        # Handle QUIC StreamDataReceived for MOQT control and object data messages
        if isinstance(event, StreamDataReceived):
            if event.stream_id == self._control_stream_id:
                logger.debug(f"QUIC EVENT: control stream {event.stream_id}: data: 0x{event.data.hex()}")
                self._handle_control_message(event.data)
                return
            elif event.stream_id in self._streams:
                logger.debug(f"QUIC EVENT: data stream {event.stream_id}: data: 0x{event.data.hex()}")
                return

        # pass H3 events to H3 API
        if self._h3 is not None:
            try:
                for h3_event in self._h3.handle_event(event):
                    self._h3_event_received(h3_event)
            except Exception as e:
                logger.error(f"QUIC EVENT: error handling event: {e}")
                raise
        else:
            logger.error(f"QUIC EVENT: stream {event.stream_id}: event not handled ({event.__class__})")

    def _h3_event_received(self, event: QuicEvent) -> None:
        if isinstance(event, HeadersReceived):
            status = None
            logger.info(f"H3 EVENT: stream {event.stream_id} HeadersReceived:")
            for name, value in event.headers:
                logger.info(f"  {name.decode()}: {value.decode()}")
                if name == b':status':
                    status = value
                                    
            if status == b"200":
                logger.info(f"H3 EVENT: stream {event.stream_id}: WebTransport session established")
                self._wt_session_setup.set()
            else:
                error = f"H3 EVENT: stream {event.stream_id}: WebTransport session setup failed ({status})"
                logger.error(error)
                raise Exception(error)

        elif isinstance(event, DataReceived):
            logger.info(f"H3 EVENT: stream {event.stream_id}: DataReceived")
            if hasattr(event, 'data'):
                logger.debug(f"H3 EVENT: stream {event.stream_id}: data: 0x{event.data.hex()} (len:{len(event.data)})")

        else:
            logger.error(f"H3 EVENT: stream {event.stream_id}: event not handled ({event.__class__})")
            if hasattr(event, 'data'):
                logger.debug(f"H3 EVENT: stream {event.stream_id}: data: 0x{event.data.hex()} (len:{len(event.data)})")

    
    def _handle_data_message(self, stream_id: int, data: bytes) -> None:
        if not data:
            logger.error(f'MOQT: stream {stream_id}: message contains no data')
            return
        try:
            # Detect stream type
            if len(data) > 0:
                stream_type = data[0]
                logger.info(f"MOQT: stream {stream_id}: type: {hex(stream_type)}")
                
                if stream_type == 0x4:  # STREAM_HEADER_SUBGROUP
                    logger.info("  Message type: STREAM_HEADER_SUBGROUP")
                    if len(data) >= 13:
                        group_id = int.from_bytes(data[1:5], 'big')
                        subgroup_id = int.from_bytes(data[5:9], 'big')
                        priority = data[9]
                        logger.info(f"  Group: {group_id}")
                        logger.info(f"  Subgroup: {subgroup_id}")
                        logger.info(f"  Priority: {priority}")

            # Basic parsing of object data
            group_id = int.from_bytes(data[0:4], 'big')
            subgroup_id = int.from_bytes(data[4:8], 'big')
            object_id = int.from_bytes(data[8:12], 'big')
            
            # Update statistics
            self._groups[group_id]['objects'] += 1
            self._groups[group_id]['subgroups'].add(subgroup_id)
            
            logger.info(f"  Object received:")
            logger.info(f"    Group: {group_id}")
            logger.info(f"    Subgroup: {subgroup_id}")
            logger.info(f"    Object: {object_id}")
            logger.info(f"    Payload size: {len(data[12:])}")
        except Exception as e:
            logger.error(f"Error processing data message: {e}")

    def _handle_control_message(self, data: bytes) -> None:
        if not data:
            return
        try:
            # Create a buffer from the data for proper QUIC varint decoding
            buffer = Buffer(data=data)
            msg_type = buffer.pull_uint_var()
            logger.info(f"MOQT control message type: {hex(msg_type)}")
            
            if msg_type == 0x41:  # SERVER_SETUP
                logger.info("MOQT control message: SERVER_SETUP")
                length = buffer.pull_uint_var()
                logger.debug(f"  Length field: {length}")
                version = buffer.pull_uint_var()
                logger.debug(f"  Selected version: {hex(version)}")
                param_count = buffer.pull_uint_var()
                logger.debug(f"  Parameter count: {param_count}")
                    
                for i in range(param_count):
                    param_id = buffer.pull_uint_var()
                    param_len = buffer.pull_uint_var()
                    param_value = buffer.pull_bytes(param_len)
                    logger.debug(f"  Parameter {i+1}: id={param_id}, len={param_len}, value=0x{param_value.hex()}")
                    
                self._moqt_session_setup.set()
            elif msg_type == 0x04:  # SUBSCRIBE_OK
                logger.info("MOQT control message: SUBSCRIBE_OK")
                if len(data) > 1:
                    logger.debug(f"  Raw subscribe data: 0x{data[1:].hex()}")
            elif msg_type == 0x05:  # SUBSCRIBE_ERROR
                logger.info("MOQT control message: SUBSCRIBE_ERROR")
                if len(data) > 1:
                    logger.debug(f"  Raw subscribe data: 0x{data[1:].hex()}")
            elif msg_type == 0x0B:  # SUBSCRIBE_DONE
                logger.info("MOQT control message: SUBSCRIBE_DONE")
                if len(data) > 1:
                    logger.debug(f"  Raw done data: 0x{data[1:].hex()}")
                
        except Exception as e:
            logger.error(f"Error parsing control message: {e}")
            logger.info(f"Raw message data: 0x{data.hex()}")

    def _encode_varint(self, value: int) -> bytes:
        if value <= 0x3f:  # 6 bits
            return bytes([value])
        elif value <= 0x3fff:  # 14 bits
            return bytes([(value >> 8) | 0x40, value & 0xff])
        elif value <= 0x3fffffff:  # 30 bits
            return bytes([
           (value >> 24) | 0x80,
           (value >> 16) & 0xff,
           (value >> 8) & 0xff,
           value & 0xff
       ])
        else:  # 62 bits
            return bytes([
           (value >> 56) | 0xc0,
           (value >> 48) & 0xff,
           (value >> 40) & 0xff,
           (value >> 32) & 0xff,
           (value >> 24) & 0xff,
           (value >> 16) & 0xff,
           (value >> 8) & 0xff,
           value & 0xff
       ])
    
    def transmit(self) -> None:
        logger.debug("Transmitting data")
        super().transmit()

    async def initialize(self, host: str, port: int) -> None:
        session_stream_id = self._h3._quic.get_next_available_stream_id(is_unidirectional=False)
        headers = [
            (b":method", b"CONNECT"),
            (b":protocol", b"webtransport"),
            (b":scheme", b"https"),
            (b":authority", f"{host}:{port}".encode()),
            (b":path", b"/moq"),
            (b"sec-webtransport-http3-draft", b"draft02"),
            (b"user-agent", USER_AGENT.encode()),
        ]
        
        logger.info(f"Sending WebTransport session request (stream: {session_stream_id})")
        self._h3.send_headers(stream_id=session_stream_id, headers=headers, end_stream=False)
        
        await self._wt_session_setup.wait()
           
        # Create bidirectional MOQT control stream 
        self._control_stream_id = self._h3.create_webtransport_stream(session_id=session_stream_id)        
        logger.info(f"Sending CLIENT_SETUP (control stream: {self._control_stream_id})")
        
        # Encode CLIENT_SETUP 
        setup_msg = bytearray()
        setup_msg.extend(self._encode_varint(0x40))
        payload = bytearray()
        payload.extend(self._encode_varint(1)) # num versions
        payload.extend(self._encode_varint(0xff000007)) # draft-07
        payload.extend(self._encode_varint(0)) # num params
        # Add Message Length and Payload
        setup_msg.extend(self._encode_varint(len(payload)))
        setup_msg.extend(payload)
        
        logger.debug(f"CLIENT_SETUP msg data: 0x{bytes(setup_msg).hex()}")
        self._h3._quic.send_stream_data(stream_id=self._control_stream_id, data=bytes(setup_msg), end_stream=False)

        try:
            await asyncio.wait_for(self._moqt_session_setup.wait(), timeout=5.0)
            logger.info("MOQT session setup complete")
        except asyncio.TimeoutError:
            logger.error("MOQT session setup error (asyncio.TimeoutError)")
            exit(1)

    async def subscribe(self, namespace: bytes, track_name: bytes) -> None:
        msg = bytearray()
        msg.append(0x3)  # Type = SUBSCRIBE
        
        # Build message body
        body = bytearray()
        body.extend(self._encode_varint(1))  # Subscribe ID
        body.extend(self._encode_varint(1))  # Track Alias
        
        # Namespace as single-element tuple
        body.extend(self._encode_varint(1))  # Namespace tuple size = 1
        body.extend(self._encode_varint(len(namespace)))
        body.extend(namespace)
        
        # Track name
        body.extend(self._encode_varint(len(track_name)))
        body.extend(track_name)
        
        # Priority and order
        body.append(128)  # Mid-priority
        body.append(0x1)  # Ascending order
        
        # Filter type = Latest Group (0x1)
        body.extend(self._encode_varint(0x1))
        
        # No parameters
        body.extend(self._encode_varint(0))
        
        # Add length and body
        msg.extend(self._encode_varint(len(body)))
        msg.extend(body)
        
        self._h3._quic.send_stream_data(
            stream_id=self._control_stream_id,
            data=bytes(msg),
            end_stream=False
        )
        self.transmit()
        logger.info(f"Sent SUBSCRIBE for {namespace}/{track_name}")

    async def unsubscribe(self) -> None:
        msg = bytearray()
        msg.append(0xA)  # Type = UNSUBSCRIBE
        msg.extend(self._encode_varint(2))  # Length
        msg.extend(self._encode_varint(1))  # Subscribe ID
        
        self._h3._quic.send_stream_data(
            stream_id=self._control_stream_id,
            data=bytes(msg),
            end_stream=False
        )        
        self.transmit()
        logger.info("Sent UNSUBSCRIBE")

class QuicLoggerCustom(QuicLogger):
    def __init__(self):
        super().__init__()
        self.logger = logging.getLogger('quic_logger')
        handler = logging.StreamHandler(sys.stderr)
        handler.setLevel(logging.DEBUG)
        self.logger.addHandler(handler)
        self.logger.setLevel(logging.DEBUG)

    def log_event(self, event_type: str, data: dict) -> None:
        self.logger.debug(f"QUIC Event: {event_type}")
        self.logger.debug(json.dumps(data, indent=2))
            
async def main(host: str, port: int, namespace: str, trackname: str, timeout: int,
               debug: bool):
    
    configuration = QuicConfiguration(
        alpn_protocols=H3_ALPN,
        is_client=True,
        verify_mode=ssl.CERT_NONE,
        quic_logger=QuicLoggerCustom() if debug else None
    )
    
    logging.getLogger().setLevel(logging.DEBUG if debug else logging.INFO)
    
    try:
        async with connect(host, port, configuration=configuration, create_protocol=MoqtClientProtocol) as client:
            await asyncio.wait_for(client.initialize(host, port), timeout=30)
            await client.subscribe(namespace.encode(), trackname.encode())
            await asyncio.sleep(timeout)
            await client.unsubscribe()
            await asyncio.sleep(5)  # Wait for SUBSCRIBE_DONE

            logger.info("\nFinal Statistics:")
            for group_id, stats in client._groups.items():
                logger.info(f"Group {group_id}:")
                logger.info(f"  Objects: {stats['objects']}")
                logger.info(f"  Subgroups: {sorted(stats['subgroups'])}")
                    
    except asyncio.TimeoutError:
        logger.error("Operation timed out")
        raise

def parse_args():
    parser = argparse.ArgumentParser(description='MOQT WebTransport Client')
    parser.add_argument('--host', type=str, default='localhost',
                      help='Host to connect to')
    parser.add_argument('--port', type=int, default=4433,
                      help='Port to connect to')
    parser.add_argument('--namespace', type=str, required=True,
                      help='Track namespace')
    parser.add_argument('--trackname', type=str, required=True,
                      help='Track name')
    parser.add_argument('--timeout', type=int, default=30,
                      help='How long to run before unsubscribing (seconds)')
    parser.add_argument('--debug', action='store_true', help='Enable debug output')
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_args()
    asyncio.run(main(
        host=args.host,
        port=args.port,
        namespace=args.namespace,
        trackname=args.trackname,
        timeout=args.timeout,
        debug=args.debug
    ))