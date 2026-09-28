use futures::{pin_mut, FutureExt, StreamExt};

use h2_support::prelude::*;
use h2_support::DEFAULT_WINDOW_SIZE;
use std::task::Context;

#[tokio::test]
async fn single_stream_send_large_body() {
    h2_support::trace_init!();

    let payload = vec![0; 1024];

    let mock = mock_io::Builder::new()
        .handshake()
        .write(frames::SETTINGS_ACK)
        .write(&[
            // POST /
            0, 0, 16, 1, 4, 0, 0, 0, 1, 131, 135, 65, 139, 157, 41, 172, 75, 143, 168, 233, 25, 151,
            33, 233, 132,
        ])
        .write(&[
            // DATA
            0, 4, 0, 0, 1, 0, 0, 0, 1,
        ])
        .write(&payload[..])
        // Read response
        .read(&[0, 0, 1, 1, 5, 0, 0, 0, 1, 0x89])
        .build();

    let (mut client, mut h2) = client::handshake(mock).await.unwrap();

    let waker = futures::task::noop_waker();
    let mut cx = Context::from_waker(&waker);
    // Poll h2 once to get notifications
    loop {
        // Run the connection until all work is done, this handles processing
        // the handshake.
        if !h2.poll_unpin(&mut cx).is_ready() {
            break;
        }
    }

    let request = Request::builder()
        .method(Method::POST)
        .uri("https://http2.akamai.com/")
        .body(())
        .unwrap();

    let (response, mut stream) = client.send_request(request, false).unwrap();

    // Reserve capacity to send the payload
    stream.reserve_capacity(payload.len());

    // The capacity should be immediately allocated
    let mut stream = h2
        .drive(util::wait_for_capacity(stream, payload.len()))
        .await;

    // Send the data
    stream.send_data(payload.into(), true).unwrap();

    // Get the response
    let resp = h2.run(response).await.unwrap();
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);

    h2.await.unwrap();
}

#[tokio::test]
async fn multiple_streams_with_payload_greater_than_default_window() {
    h2_support::trace_init!();

    let payload = vec![0; 16384 * 5 - 1];
    let payload_clone = payload.clone();

    let (io, mut srv) = mock::new();

    let srv = async move {
        let settings = srv.assert_client_handshake().await;
        assert_default_settings!(settings);
        srv.recv_frame(frames::headers(1).request("POST", "https://http2.akamai.com/"))
            .await;
        srv.recv_frame(frames::headers(3).request("POST", "https://http2.akamai.com/"))
            .await;
        srv.recv_frame(frames::headers(5).request("POST", "https://http2.akamai.com/"))
            .await;
        srv.recv_frame(frames::data(1, &payload[0..16_384])).await;
        srv.recv_frame(frames::data(1, &payload[16_384..(16_384 * 2)]))
            .await;
        srv.recv_frame(frames::data(1, &payload[(16_384 * 2)..(16_384 * 3)]))
            .await;
        srv.recv_frame(frames::data(1, &payload[(16_384 * 3)..(16_384 * 4 - 1)]))
            .await;
        srv.send_frame(frames::settings()).await;
        srv.recv_frame(frames::settings_ack()).await;
        srv.send_frame(frames::headers(1).response(200).eos()).await;
        srv.send_frame(frames::headers(3).response(200).eos()).await;
        srv.send_frame(frames::headers(5).response(200).eos()).await;
    };

    let client = async move {
        let (mut client, mut conn) = client::handshake(io).await.unwrap();
        let request1 = Request::post("https://http2.akamai.com/").body(()).unwrap();
        let request2 = Request::post("https://http2.akamai.com/").body(()).unwrap();
        let request3 = Request::post("https://http2.akamai.com/").body(()).unwrap();
        let (response1, mut stream1) = client.send_request(request1, false).unwrap();
        let (_response2, mut stream2) = client.send_request(request2, false).unwrap();
        let (_response3, mut stream3) = client.send_request(request3, false).unwrap();

        // The capacity should be immediately
        // allocated to default window size (smaller than payload)
        stream1.reserve_capacity(payload_clone.len());
        let mut stream1 = conn
            .drive(util::wait_for_capacity(stream1, DEFAULT_WINDOW_SIZE))
            .await;

        stream2.reserve_capacity(payload_clone.len());
        assert_eq!(stream2.capacity(), 0);

        stream3.reserve_capacity(payload_clone.len());
        assert_eq!(stream3.capacity(), 0);

        stream1.send_data(payload_clone.into(), true).unwrap();

        // hold onto streams so they don't close
        // stream1 doesn't close because response1 is used
        let _res = conn.drive(response1).await.expect("response");
        conn.await.expect("client");
    };

    join(srv, client).await;
}

#[tokio::test]
async fn single_stream_send_extra_large_body_multi_frames_one_buffer() {
    h2_support::trace_init!();

    let payload = vec![0; 32_768];

    let mock = mock_io::Builder::new()
        .handshake()
        .write(frames::SETTINGS_ACK)
        .write(&[
            // POST /
            0, 0, 16, 1, 4, 0, 0, 0, 1, 131, 135, 65, 139, 157, 41, 172, 75, 143, 168, 233, 25, 151,
            33, 233, 132,
        ])
        .write(&[
            // DATA
            0, 64, 0, 0, 0, 0, 0, 0, 1,
        ])
        .write(&payload[0..16_384])
        .write(&[
            // DATA
            0, 64, 0, 0, 1, 0, 0, 0, 1,
        ])
        .write(&payload[16_384..])
        // Read response
        .read(&[0, 0, 1, 1, 5, 0, 0, 0, 1, 0x89])
        .build();

    let (mut client, mut h2) = client::handshake(mock).await.unwrap();

    let waker = futures::task::noop_waker();
    let mut cx = Context::from_waker(&waker);
    // Poll h2 once to get notifications
    loop {
        // Run the connection until all work is done, this handles processing
        // the handshake.
        if !h2.poll_unpin(&mut cx).is_ready() {
            break;
        }
    }

    let request = Request::builder()
        .method(Method::POST)
        .uri("https://http2.akamai.com/")
        .body(())
        .unwrap();

    let (response, mut stream) = client.send_request(request, false).unwrap();

    stream.reserve_capacity(payload.len());

    // The capacity should be immediately allocated
    let mut stream = h2
        .drive(util::wait_for_capacity(stream, payload.len()))
        .await;

    // Send the data
    stream.send_data(payload.into(), true).unwrap();

    // Get the response
    let resp = h2.run(response).await.unwrap();
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);

    h2.await.unwrap();
}

#[tokio::test]
async fn single_stream_send_body_greater_than_default_window() {
    h2_support::trace_init!();

    let payload = vec![0; 16384 * 5 - 1];

    let mock = mock_io::Builder::new()
        .handshake()
        .write(frames::SETTINGS_ACK)
        .write(&[
            // POST /
            0, 0, 16, 1, 4, 0, 0, 0, 1, 131, 135, 65, 139, 157, 41, 172, 75, 143, 168, 233, 25, 151,
            33, 233, 132,
        ])
        .write(&[
            // DATA
            0, 64, 0, 0, 0, 0, 0, 0, 1,
        ])
        .write(&payload[0..16_384])
        .write(&[
            // DATA
            0, 64, 0, 0, 0, 0, 0, 0, 1,
        ])
        .write(&payload[16_384..(16_384 * 2)])
        .write(&[
            // DATA
            0, 64, 0, 0, 0, 0, 0, 0, 1,
        ])
        .write(&payload[(16_384 * 2)..(16_384 * 3)])
        .write(&[
            // DATA
            0, 63, 255, 0, 0, 0, 0, 0, 1,
        ])
        .write(&payload[(16_384 * 3)..(16_384 * 4 - 1)])
        // Read window update
        .read(&[0, 0, 4, 8, 0, 0, 0, 0, 0, 0, 0, 64, 0])
        .read(&[0, 0, 4, 8, 0, 0, 0, 0, 1, 0, 0, 64, 0])
        .write(&[
            // DATA
            0, 64, 0, 0, 1, 0, 0, 0, 1,
        ])
        .write(&payload[(16_384 * 4 - 1)..(16_384 * 5 - 1)])
        // Read response
        .read(&[0, 0, 1, 1, 5, 0, 0, 0, 1, 0x89])
        .build();

    let (mut client, mut h2) = client::handshake(mock).await.unwrap();

    let waker = futures::task::noop_waker();
    let mut cx = Context::from_waker(&waker);
    // Poll h2 once to get notifications
    loop {
        // Run the connection until all work is done, this handles processing
        // the handshake.
        if !h2.poll_unpin(&mut cx).is_ready() {
            break;
        }
    }

    let request = Request::builder()
        .method(Method::POST)
        .uri("https://http2.akamai.com/")
        .body(())
        .unwrap();

    let (response, mut stream) = client.send_request(request, false).unwrap();

    // Flush request head
    loop {
        // Run the connection until all work is done, this handles processing
        // the handshake.
        if !h2.poll_unpin(&mut cx).is_ready() {
            break;
        }
    }

    // Send the data
    stream.send_data(payload.into(), true).unwrap();

    // Get the response
    let resp = h2.run(response).await.unwrap();
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);

    h2.await.unwrap();
}

#[tokio::test]
async fn single_stream_send_extra_large_body_multi_frames_multi_buffer() {
    h2_support::trace_init!();

    let payload = vec![0; 32_768];

    let mock = mock_io::Builder::new()
        // .handshake()
        .write(b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n")
        .write(frames::SETTINGS)
        .read(frames::SETTINGS)
        // Add wait to force the data writes to chill
        .wait(Duration::from_millis(10))
        // Rest
        .write(&[
            // POST /
            0, 0, 16, 1, 4, 0, 0, 0, 1, 131, 135, 65, 139, 157, 41, 172, 75, 143, 168, 233, 25, 151,
            33, 233, 132,
        ])
        .write(frames::SETTINGS_ACK)
        .read(frames::SETTINGS_ACK)
        .write(&[
            // DATA
            0, 64, 0, 0, 0, 0, 0, 0, 1,
        ])
        .write(&payload[0..16_384])
        .wait(Duration::from_millis(10))
        .write(&[
            // DATA
            0, 64, 0, 0, 1, 0, 0, 0, 1,
        ])
        .write(&payload[16_384..])
        // Read response
        .read(&[0, 0, 1, 1, 5, 0, 0, 0, 1, 0x89])
        .build();

    let (mut client, mut h2) = client::handshake(mock).await.unwrap();

    let request = Request::builder()
        .method(Method::POST)
        .uri("https://http2.akamai.com/")
        .body(())
        .unwrap();

    let (response, mut stream) = client.send_request(request, false).unwrap();

    stream.reserve_capacity(payload.len());

    // The capacity should be immediately allocated
    let mut stream = h2
        .drive(util::wait_for_capacity(stream, payload.len()))
        .await;

    // Send the data
    stream.send_data(payload.into(), true).unwrap();

    // Get the response
    let resp = h2.run(response).await.unwrap();

    assert_eq!(resp.status(), StatusCode::NO_CONTENT);

    h2.await.unwrap();
}

#[tokio::test]
async fn send_data_receive_window_update() {
    h2_support::trace_init!();
    let (m, mut mock) = mock::new();

    let h2 = async move {
        let (mut client, mut h2) = client::handshake(m).await.unwrap();
        let request = Request::builder()
            .method(Method::POST)
            .uri("https://http2.akamai.com/")
            .body(())
            .unwrap();

        // Send request
        let (_response, mut stream) = client.send_request(request, false).unwrap();

        // Send data frame
        stream.send_data("hello".into(), false).unwrap();

        stream.reserve_capacity(frame::DEFAULT_INITIAL_WINDOW_SIZE as usize);

        // Wait for capacity
        let mut stream = h2
            .drive(util::wait_for_capacity(
                stream,
                frame::DEFAULT_INITIAL_WINDOW_SIZE as usize,
            ))
            .await;
        let payload = vec![0; frame::DEFAULT_INITIAL_WINDOW_SIZE as usize];
        stream.send_data(payload.into(), true).unwrap();

        // keep `stream` from being dropped in order to prevent
        // it from sending an RST_STREAM frame.
        std::mem::forget(stream);
        h2.await.unwrap();
    };

    let mock = async move {
        let _ = mock.assert_client_handshake().await;

        let frame = mock.next().await.unwrap();
        let request = assert_headers!(frame.unwrap());
        assert!(!request.is_end_stream());
        let frame = mock.next().await.unwrap();
        let data = assert_data!(frame.unwrap());

        // Update the windows
        let len = data.payload().len();
        let f = frame::WindowUpdate::new(StreamId::zero(), len as u32);
        mock.send(f.into()).await.unwrap();

        let f = frame::WindowUpdate::new(data.stream_id(), len as u32);
        mock.send(f.into()).await.unwrap();

        for _ in 0..3usize {
            let frame = mock.next().await.unwrap();
            let data = assert_data!(frame.unwrap());
            assert_eq!(data.payload().len(), frame::DEFAULT_MAX_FRAME_SIZE as usize);
        }
        let frame = mock.next().await.unwrap();
        let data = assert_data!(frame.unwrap());
        assert_eq!(
            data.payload().len(),
            (frame::DEFAULT_MAX_FRAME_SIZE - 1) as usize
        );
    };

    join(mock, h2).await;
}

#[tokio::test]
async fn stream_count_over_max_stream_limit_does_not_starve_capacity() {
    use tokio::sync::oneshot;

    h2_support::trace_init!();

    let (io, mut srv) = mock::new();

    let (tx, rx) = oneshot::channel();

    let srv = async move {
        let _ = srv
            .assert_client_handshake_with_settings(
                frames::settings()
                    // super tiny server
                    .max_concurrent_streams(1),
            )
            .await;
        srv.recv_frame(frames::headers(1).request("POST", "http://example.com/"))
            .await;

        srv.recv_frame(frames::data(1, vec![0; 16384])).await;
        srv.recv_frame(frames::data(1, vec![0; 16384])).await;
        srv.recv_frame(frames::data(1, vec![0; 16384])).await;
        srv.recv_frame(frames::data(1, vec![0; 16383]).eos()).await;
        srv.send_frame(frames::headers(1).response(200).eos()).await;

        // All of these connection capacities should be assigned to stream 3
        srv.send_frame(frames::window_update(0, 16384)).await;
        srv.send_frame(frames::window_update(0, 16384)).await;
        srv.send_frame(frames::window_update(0, 16384)).await;
        srv.send_frame(frames::window_update(0, 16383)).await;

        // StreamId(3) should be able to send all of its request with the conn capacity
        srv.recv_frame(frames::headers(3).request("POST", "http://example.com/"))
            .await;
        srv.recv_frame(frames::data(3, vec![0; 16384])).await;
        srv.recv_frame(frames::data(3, vec![0; 16384])).await;
        srv.recv_frame(frames::data(3, vec![0; 16384])).await;
        srv.recv_frame(frames::data(3, vec![0; 16383]).eos()).await;
        srv.send_frame(frames::headers(3).response(200).eos()).await;

        // Then all the future stream is guaranteed to be send-able by induction
        tx.send(()).unwrap();
    };

    fn request() -> Request<()> {
        Request::builder()
            .method(Method::POST)
            .uri("http://example.com/")
            .body(())
            .unwrap()
    }

    let client = async move {
        let (mut client, mut conn) = client::Builder::new()
            .handshake::<_, Bytes>(io)
            .await
            .expect("handshake");

        let (req1, mut send1) = client.send_request(request(), false).unwrap();
        let (req2, mut send2) = client.send_request(request(), false).unwrap();

        // Use up the connection window.
        send1.send_data(vec![0; 65535].into(), true).unwrap();
        // Queue up for more connection window.
        send2.send_data(vec![0; 65535].into(), true).unwrap();

        // Queue up more pending open streams
        for _ in 0..5 {
            let (_, mut send) = client.send_request(request(), false).unwrap();
            send.send_data(vec![0; 65535].into(), true).unwrap();
        }

        let response = conn.drive(req1).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let response = conn.drive(req2).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let _ = rx.await;
    };

    let task = join(srv, client);
    pin_mut!(task);

    let t = tokio::time::sleep(Duration::from_secs(5)).map(|_| panic!("time out"));
    pin_mut!(t);

    select(task, t).await;
}

fn request_with_dependency(path: &str, weight: u8) -> Request<()> {
    Request::builder()
        .uri(format!("https://example.com{path}"))
        .extension(h2::frame::StreamDependency::new(
            StreamId::ZERO,
            weight,
            true,
        ))
        .body(())
        .unwrap()
}

#[tokio::test]
async fn request_dependency_chains_to_open_streams() {
    h2_support::trace_init!();
    let (io, mut srv) = mock::new();

    let srv = async move {
        let settings = srv.assert_client_handshake().await;
        assert_default_settings!(settings);
        // Nothing open: the first stream depends on stream 0.
        srv.recv_frame(
            frames::headers(1)
                .request("GET", "https://example.com/a")
                .stream_dependency(0, 255, true)
                .eos(),
        )
        .await;
        // Lower weight: the open stream 1.
        srv.recv_frame(
            frames::headers(3)
                .request("GET", "https://example.com/b")
                .stream_dependency(1, 219, true)
                .eos(),
        )
        .await;
        // Same weight as 3: the latest stream of that weight.
        srv.recv_frame(
            frames::headers(5)
                .request("GET", "https://example.com/c")
                .stream_dependency(3, 219, true)
                .eos(),
        )
        .await;
        // Highest weight: stream 1, although 3 and 5 are newer.
        srv.recv_frame(
            frames::headers(7)
                .request("GET", "https://example.com/d")
                .stream_dependency(1, 255, true)
                .eos(),
        )
        .await;
        srv.send_frame(frames::headers(5).response(200).eos()).await;
        srv.send_frame(frames::headers(3).response(200).eos()).await;
        // 3 and 5 are closed: the lowest open weight above 182 is 255.
        srv.recv_frame(
            frames::headers(9)
                .request("GET", "https://example.com/e")
                .stream_dependency(7, 182, true)
                .eos(),
        )
        .await;
        srv.send_frame(frames::headers(1).response(200).eos()).await;
        srv.send_frame(frames::headers(7).response(200).eos()).await;
        srv.send_frame(frames::headers(9).response(200).eos()).await;
    };

    let h2 = async move {
        let (mut client, mut h2) = client::Builder::new()
            .stream_dependency_chain(true)
            .handshake::<_, Bytes>(io)
            .await
            .expect("handshake");

        let mut send = |path: &str, weight: u8| {
            client
                .send_request(request_with_dependency(path, weight), true)
                .unwrap()
                .0
        };
        let a = send("/a", 255);
        let b = send("/b", 219);
        let c = send("/c", 219);
        let d = send("/d", 255);
        h2.drive(c).await.unwrap();
        h2.drive(b).await.unwrap();
        let e = send("/e", 182);
        h2.drive(a).await.unwrap();
        h2.drive(d).await.unwrap();
        h2.drive(e).await.unwrap();
        drop(send);
        drop(client);
        h2.await.expect("client");
    };

    join(srv, h2).await;
}

#[tokio::test]
async fn request_dependency_replaces_connection_dependency() {
    h2_support::trace_init!();
    let (io, mut srv) = mock::new();

    let srv = async move {
        let settings = srv.assert_client_handshake().await;
        assert_default_settings!(settings);
        srv.recv_frame(
            frames::headers(1)
                .request("GET", "https://example.com/")
                .stream_dependency(0, 41, false)
                .eos(),
        )
        .await;
        srv.recv_frame(
            frames::headers(3)
                .request("GET", "https://example.com/img")
                .stream_dependency(0, 11, false)
                .eos(),
        )
        .await;
        srv.send_frame(frames::headers(1).response(200).eos()).await;
        srv.send_frame(frames::headers(3).response(200).eos()).await;
    };

    let h2 = async move {
        let (mut client, mut h2) = client::Builder::new()
            .headers_stream_dependency(h2::frame::StreamDependency::new(StreamId::ZERO, 41, false))
            .handshake::<_, Bytes>(io)
            .await
            .expect("handshake");

        let (first, _) = client
            .send_request(Request::get("https://example.com/").body(()).unwrap(), true)
            .unwrap();
        let image = Request::builder()
            .uri("https://example.com/img")
            .extension(h2::frame::StreamDependency::new(StreamId::ZERO, 11, false))
            .body(())
            .unwrap();
        let (second, _) = client.send_request(image, true).unwrap();
        h2.drive(first).await.unwrap();
        h2.drive(second).await.unwrap();
        drop(client);
        h2.await.expect("client");
    };

    join(srv, h2).await;
}

#[tokio::test]
async fn priority_frames_go_out_once() {
    h2_support::trace_init!();
    let (io, mut srv) = mock::new();

    let priority = |id: u32| {
        h2::frame::Priority::new(
            id.into(),
            h2::frame::StreamDependency::new(StreamId::ZERO, 200, false),
        )
    };

    let srv = async move {
        let settings = srv.assert_client_handshake().await;
        assert_default_settings!(settings);
        srv.recv_frame(priority(3)).await;
        srv.recv_frame(priority(5)).await;
        srv.recv_frame(
            frames::headers(7)
                .request("GET", "https://example.com/")
                .eos(),
        )
        .await;
        srv.send_frame(frames::headers(7).response(200).eos()).await;
        // The second request comes without PRIORITY frames.
        srv.recv_frame(
            frames::headers(9)
                .request("GET", "https://example.com/")
                .eos(),
        )
        .await;
        srv.send_frame(frames::headers(9).response(200).eos()).await;
    };

    let h2 = async move {
        let (mut client, mut h2) = client::Builder::new()
            .initial_stream_id(7)
            .priorities(
                h2::frame::Priorities::builder()
                    .push(priority(3))
                    .push(priority(5))
                    .build(),
            )
            .handshake::<_, Bytes>(io)
            .await
            .expect("handshake");

        for _ in 0..2 {
            let res = h2.drive(client.get("https://example.com/")).await.unwrap();
            assert_eq!(res.status(), StatusCode::OK);
        }
        drop(client);
        h2.await.expect("client");
    };

    join(srv, h2).await;
}
