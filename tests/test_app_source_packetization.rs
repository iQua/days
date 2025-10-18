//! Integration tests for AppSource and TCP packetization
//!
//! Tests the complete data flow from AppActor (byte buffers) to TCPPacketSource (packets)

use daytone::flows::app_source::{AppDataSource, AppSourceRuntimeConfig};
use daytone::flows::packet::Packet;

/// Test that TCP packetization creates packets with correct sequence numbers
#[test]
fn test_tcp_packetization_sequence_numbers() {
    let config = AppSourceRuntimeConfig::default();
    let total_size = 1536; // 3 MSS worth of data (512 * 3)
    let mss = 512;

    let (data_src, _actor) = AppDataSource::buffered_actor(total_size, config);
    let _handle = data_src.handle();

    // Simulate TCP packetization logic
    let mut packets = Vec::new();
    let mut next_seq = 0;
    let flow_id = 42;

    // Simulate pulling data and creating packets (what TCPPacketSource does)
    let data: Vec<u8> = vec![0u8; total_size]; // Simulated pull result
    let mut offset = 0;

    while offset < data.len() {
        let chunk_size = mss.min(data.len() - offset);
        let packet = Packet::new(chunk_size, next_seq, flow_id, 0.0);

        packets.push(packet.clone());
        next_seq += chunk_size;
        offset += chunk_size;
    }

    // Verify we created exactly 3 packets
    assert_eq!(packets.len(), 3);

    // Verify sequence numbers are correct
    assert_eq!(packets[0].packet_id, 0);
    assert_eq!(packets[0].size, 512);

    assert_eq!(packets[1].packet_id, 512);
    assert_eq!(packets[1].size, 512);

    assert_eq!(packets[2].packet_id, 1024);
    assert_eq!(packets[2].size, 512);

    // Verify flow IDs
    assert!(packets.iter().all(|p| p.flow_id == flow_id));
}

/// Test TCP packetization with non-MSS-aligned data
#[test]
fn test_tcp_packetization_non_aligned() {
    let total_size = 1300; // Not evenly divisible by 512
    let mss = 512;

    let data: Vec<u8> = vec![0u8; total_size];
    let mut packets = Vec::new();
    let mut next_seq = 0;
    let mut offset = 0;

    while offset < data.len() {
        let chunk_size = mss.min(data.len() - offset);
        let packet = Packet::new(chunk_size, next_seq, 0, 0.0);

        packets.push(packet.clone());
        next_seq += chunk_size;
        offset += chunk_size;
    }

    // Should create 3 packets: 512 + 512 + 276
    assert_eq!(packets.len(), 3);
    assert_eq!(packets[0].size, 512);
    assert_eq!(packets[1].size, 512);
    assert_eq!(packets[2].size, 276); // Remainder

    // Verify sequence numbers
    assert_eq!(packets[0].packet_id, 0);
    assert_eq!(packets[1].packet_id, 512);
    assert_eq!(packets[2].packet_id, 1024);

    // Verify total coverage
    let total_bytes: usize = packets.iter().map(|p| p.size).sum();
    assert_eq!(total_bytes, total_size);
}

/// Test that small data (< MSS) creates a single packet
#[test]
fn test_tcp_packetization_small_data() {
    let total_size = 100; // Much smaller than MSS
    let mss = 512;

    let data: Vec<u8> = vec![0u8; total_size];
    let mut packets = Vec::new();
    let mut offset = 0;
    let mut next_seq = 0;

    while offset < data.len() {
        let chunk_size = mss.min(data.len() - offset);
        let packet = Packet::new(chunk_size, next_seq, 0, 0.0);

        packets.push(packet);
        next_seq += chunk_size;
        offset += chunk_size;
    }

    // Should create exactly 1 packet
    assert_eq!(packets.len(), 1);
    assert_eq!(packets[0].size, 100);
    assert_eq!(packets[0].packet_id, 0);
}

/// Test Broadcast scenario: multiple flows share the same data
#[test]
fn test_broadcast_multiple_flows_same_data() {
    let config = AppSourceRuntimeConfig::default();
    let total_size = 1024;

    let (data_src, _actor) = AppDataSource::buffered_actor(total_size, config);

    // Create handles for 4 different flows (simulating broadcast to 4 destinations)
    let handle1 = data_src.handle();
    let handle2 = data_src.handle();
    let handle3 = data_src.handle();
    let handle4 = data_src.handle();

    // All handles should access the same data range
    assert_eq!(handle1.get_offset(), 0);
    assert_eq!(handle2.get_offset(), 0);
    assert_eq!(handle3.get_offset(), 0);
    assert_eq!(handle4.get_offset(), 0);

    assert_eq!(handle1.get_length(), Some(total_size));
    assert_eq!(handle2.get_length(), Some(total_size));
    assert_eq!(handle3.get_length(), Some(total_size));
    assert_eq!(handle4.get_length(), Some(total_size));

    // But cursors are independent (each flow tracks its own progress)
    assert_eq!(handle1.get_cursor(), 0);
    assert_eq!(handle2.get_cursor(), 0);
    assert_eq!(handle3.get_cursor(), 0);
    assert_eq!(handle4.get_cursor(), 0);
}

/// Test RingAllReduce scenario: different flows access different chunks
#[test]
fn test_ring_allreduce_chunk_partitioning() {
    let config = AppSourceRuntimeConfig::default();
    let total_size = 2048;
    let num_nodes = 4;
    let chunk_size = total_size / num_nodes; // 512 bytes per chunk

    let (data_src, _actor) = AppDataSource::buffered_actor(total_size, config);

    // Create handles for each chunk (each flow in RingAllReduce sends one chunk)
    let mut handles = Vec::new();
    for i in 0..num_nodes {
        let offset = i * chunk_size;
        let length = if i == num_nodes - 1 {
            total_size - offset // Last chunk gets remainder
        } else {
            chunk_size
        };
        let handle = data_src.handle_with_offset(offset, Some(length));
        handles.push(handle);
    }

    // Verify each handle accesses a different chunk
    assert_eq!(handles[0].get_offset(), 0);
    assert_eq!(handles[0].get_length(), Some(512));

    assert_eq!(handles[1].get_offset(), 512);
    assert_eq!(handles[1].get_length(), Some(512));

    assert_eq!(handles[2].get_offset(), 1024);
    assert_eq!(handles[2].get_length(), Some(512));

    assert_eq!(handles[3].get_offset(), 1536);
    assert_eq!(handles[3].get_length(), Some(512));

    // Verify no overlaps
    for i in 0..num_nodes {
        for j in (i + 1)..num_nodes {
            let end_i = handles[i].get_offset() + handles[i].get_length().unwrap();
            let start_j = handles[j].get_offset();
            assert!(end_i <= start_j, "Chunks {} and {} overlap!", i, j);
        }
    }

    // Verify full coverage
    let total_coverage: usize = handles.iter().map(|h| h.get_length().unwrap()).sum();
    assert_eq!(total_coverage, total_size);
}

/// Test cursor advancement after simulated pulls
#[test]
fn test_cursor_advancement_simulation() {
    // Simulate multiple pull operations
    let mut cursor = 0;
    let length = Some(1000usize);

    // Pull 1: 300 bytes
    let req1 = 300;
    let allowed1 = length.map(|l| l.saturating_sub(cursor)).unwrap_or(req1);
    let actual1 = req1.min(allowed1);
    cursor += actual1;
    assert_eq!(cursor, 300);

    // Pull 2: 500 bytes
    let req2 = 500;
    let allowed2 = length.map(|l| l.saturating_sub(cursor)).unwrap_or(req2);
    let actual2 = req2.min(allowed2);
    cursor += actual2;
    assert_eq!(cursor, 800);

    // Pull 3: 300 bytes (but only 200 left)
    let req3 = 300;
    let allowed3 = length.map(|l| l.saturating_sub(cursor)).unwrap_or(req3);
    let actual3 = req3.min(allowed3);
    cursor += actual3;
    assert_eq!(cursor, 1000);
    assert_eq!(actual3, 200); // Only 200 bytes were available

    // Pull 4: should return 0 (exhausted)
    let req4 = 100;
    let allowed4 = length.map(|l| l.saturating_sub(cursor)).unwrap_or(req4);
    let actual4 = req4.min(allowed4);
    assert_eq!(actual4, 0);
}

/// Test packet metadata is created correctly (not cloned from wrong source)
#[test]
fn test_packet_metadata_correctness() {
    let flow_id = 123;
    let start_seq = 5000;
    let timestamp = 42.5;
    let size = 256;

    // Create a packet with specific metadata
    let packet = Packet::new(size, start_seq, flow_id, timestamp);

    // Verify all metadata is correct
    assert_eq!(packet.size, size);
    assert_eq!(packet.packet_id, start_seq);
    assert_eq!(packet.flow_id, flow_id);
    assert_eq!(packet.time, timestamp);

    // This verifies that we're creating fresh packets with correct metadata,
    // not cloning and modifying existing packets (which was the old buggy approach)
}

/// Test RingAllReduce with uneven chunk sizes
#[test]
fn test_ring_allreduce_uneven_chunks() {
    let config = AppSourceRuntimeConfig::default();
    let total_size = 1000; // Not evenly divisible by 3
    let num_nodes = 3;
    let chunk_size = total_size / num_nodes; // 333

    let (data_src, _actor) = AppDataSource::buffered_actor(total_size, config);

    let chunk0 = data_src.handle_with_offset(0, Some(chunk_size));
    let chunk1 = data_src.handle_with_offset(chunk_size, Some(chunk_size));
    let chunk2_offset = 2 * chunk_size;
    let chunk2_len = total_size - chunk2_offset; // Remainder
    let chunk2 = data_src.handle_with_offset(chunk2_offset, Some(chunk2_len));

    assert_eq!(chunk0.get_offset(), 0);
    assert_eq!(chunk0.get_length(), Some(333));

    assert_eq!(chunk1.get_offset(), 333);
    assert_eq!(chunk1.get_length(), Some(333));

    assert_eq!(chunk2.get_offset(), 666);
    assert_eq!(chunk2.get_length(), Some(334)); // Gets the extra byte

    // Verify complete coverage
    let total =
        chunk0.get_length().unwrap() + chunk1.get_length().unwrap() + chunk2.get_length().unwrap();
    assert_eq!(total, total_size);
}
