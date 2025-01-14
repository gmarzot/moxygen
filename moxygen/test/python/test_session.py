# test_session.py
from moxygen.common import SessionPair, SubscribeErrorCode, SubscribeDoneStatusCode, GroupOrder, run_with_timeout
from moxygen.moqsession import MoqSession, SetupParams
import threading
import time
import pytest

def test_moq_session():
    client, server = MoqSession.create_session_pair()
    with SessionPair(client, server) as pair:
        result = run_with_timeout(pair.client.subscribe, track_name="test/video")

def test_basic_subscribe():
    client, server = MoqSession.create_session_pair()
    
    # Start sessions
    client.start()
    server.start()
    
    # Setup sessions
    params = SetupParams(max_subscribe_id=100)
    client.setup_client(params)
    server.setup_server(params)
    
    # Run event base in background
    event_thread = threading.Thread(target=client.run_event_base)
    event_thread.daemon = True
    event_thread.start()

    # Subscribe to a track
    track_name = "test/video"
    subscription = client.subscribe(
        track_name,
        group_order=GroupOrder.INCREASING
    )
    
    # Give some time for message processing
    time.sleep(0.1)
    client.run_one_event()
    
    # Verify subscription was successful
    assert subscription.track_name == track_name
    assert subscription.is_active()
    
    # Cleanup
    client.close()
    server.close()
    event_thread.join(timeout=1.0)

def test_announce_subscribe():
    client, server = MoqSession.create_session_pair()
    
    client.start()
    server.start()
    
    params = SetupParams(max_subscribe_id=100)
    client.setup_client(params)
    server.setup_server(params)
    
    event_thread = threading.Thread(target=client.run_event_base)
    event_thread.daemon = True
    event_thread.start()

    # Server announces a track
    track_namespace = "live"
    track_name = "camera1"
    announce_result = server.announce(
        track_namespace=track_namespace,
        track_name=track_name,
        group_order=GroupOrder.INCREASING
    )
    
    # Client subscribes to announces
    client.subscribe_announces(track_namespace)
    time.sleep(0.1)
    client.run_one_event()
    
    # Client should see the announced track
    track = client.get_announced_track(f"{track_namespace}/{track_name}")
    assert track is not None
    
    # Subscribe to the announced track
    subscription = client.subscribe(
        track.full_name,
        group_order=GroupOrder.INCREASING
    )
    time.sleep(0.1)
    client.run_one_event()
    
    assert subscription.is_active()
    
    # Cleanup
    client.close()
    server.close()
    event_thread.join(timeout=1.0)

def test_track_data_flow():
    client, server = MoqSession.create_session_pair()
    
    client.start()
    server.start()
    
    params = SetupParams(max_subscribe_id=100)
    client.setup_client(params)
    server.setup_server(params)
    
    event_thread = threading.Thread(target=client.run_event_base)
    event_thread.daemon = True
    event_thread.start()

    # Server announces and client subscribes
    track_name = "test/video"
    server.announce(track_name)
    subscription = client.subscribe(track_name)
    
    # Server publishes some objects
    test_data = b"test object data"
    server.publish(
        track_name=track_name,
        group_id=1,
        object_id=1,
        data=test_data
    )
    
    # Give time for delivery
    time.sleep(0.1)
    client.run_one_event()
    
    # Check received data
    received_objects = list(subscription.get_objects())
    assert len(received_objects) == 1
    assert received_objects[0].data == test_data
    
    # Cleanup
    client.close() 
    server.close()
    event_thread.join(timeout=1.0)

def test_subscribe_error_handling():
    client, server = MoqSession.create_session_pair()
    
    client.start()
    server.start()
    
    params = SetupParams(max_subscribe_id=100)
    client.setup_client(params)
    server.setup_server(params)
    
    event_thread = threading.Thread(target=client.run_event_base)
    event_thread.daemon = True
    event_thread.start()

    # Try to subscribe to non-existent track
    try:
        subscription = client.subscribe("nonexistent/track")
        time.sleep(0.1)
        client.run_one_event()
    except SubscribeError as e:
        assert "Track not found" in str(e)
    
    # Cleanup
    client.close()
    server.close()
    event_thread.join(timeout=1.0)


def test_subscribe_success():
    client, server = MoqSession.create_session_pair()
    with SessionPair(client, server) as pair:
        # Subscribe returns a Python-friendly track handle
        track = pair.client.subscribe("test/video")
        assert track.name == "test/video"
        assert track.group_order == GroupOrder.Default

def test_subscribe_error():
    client, server = MoqSession.create_session_pair()
    with SessionPair(client, server) as pair:
        with pytest.raises(SubscribeException) as exc_info:
            pair.client.subscribe("nonexistent/track")
        
        error = exc_info.value
        assert error.error_code == SubscribeErrorCode.TRACK_NOT_EXIST
        assert isinstance(error.reason, str)
        assert error.retry_alias is None

def test_subscribe_done():
    client, server = MoqSession.create_session_pair()
    with SessionPair(client, server) as pair:
        track = pair.client.subscribe("test/video")
        
        # Server ends subscription
        server.end_track("test/video", final_group=10, final_object=20)
        
        done_info = track.wait_for_done()
        assert done_info['status_code'] == SubscribeDoneStatusCode.TRACK_ENDED
        assert done_info['final_object']['group'] == 10
        assert done_info['final_object']['object'] == 20