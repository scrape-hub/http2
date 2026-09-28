use futures::StreamExt;
use h2_support::prelude::*;

#[tokio::test]
async fn write_continuation_frames() {
    // An invalid dependency ID results in a stream level error. The hpack
    // payload should still be decoded.
    h2_support::trace_init!();
    let (io, mut srv) = mock::new();

    let large = build_large_headers();

    // Build the large request frame
    let frame = large.iter().fold(
        frames::headers(1).request("GET", "https://http2.akamai.com/"),
        |frame, &(name, ref value)| frame.field(name, &value[..]),
    );

    let srv = async move {
        let settings = srv.assert_client_handshake().await;
        assert_default_settings!(settings);
        srv.recv_frame(frame.eos()).await;
        srv.send_frame(frames::headers(1).response(204).eos()).await;
    };

    let client = async move {
        let (mut client, mut conn) = client::handshake(io).await.expect("handshake");

        let mut request = Request::builder();
        request = request.uri("https://http2.akamai.com/");

        for &(name, ref value) in &large {
            request = request.header(name, &value[..]);
        }

        let request = request.body(()).unwrap();

        let req = async {
            let res = client
                .send_request(request, true)
                .expect("send_request1")
                .0
                .await;
            let response = res.unwrap();
            assert_eq!(response.status(), StatusCode::NO_CONTENT);
        };

        conn.drive(req).await;
        conn.await.unwrap();
    };

    join(srv, client).await;
}

#[tokio::test]
async fn client_settings_header_table_size() {
    // A server sets the SETTINGS_HEADER_TABLE_SIZE to 0, test that the
    // client doesn't send indexed headers.
    h2_support::trace_init!();

    let io = mock_io::Builder::new()
        // Read SETTINGS_HEADER_TABLE_SIZE = 0
        .handshake_read_settings(&[
            0, 0, 6, // len
            4, // type
            0, // flags
            0, 0, 0, 0, // stream id
            0, 0x1, // id = SETTINGS_HEADER_TABLE_SIZE
            0, 0, 0, 0, // value = 0
        ])
        // Write GET / (1st)
        .write(&[
            0, 0, 0x10, 1, 5, 0, 0, 0, 1, 0x82, 0x87, 0x41, 0x8B, 0x9D, 0x29, 0xAC, 0x4B, 0x8F,
            0xA8, 0xE9, 0x19, 0x97, 0x21, 0xE9, 0x84,
        ])
        .write(frames::SETTINGS_ACK)
        // Read response
        .read(&[0, 0, 1, 1, 5, 0, 0, 0, 1, 137])
        // Write GET / (2nd, doesn't use indexed headers)
        // - Sends 0x20 about size change
        // - Sends :authority as literal instead of indexed
        .write(&[
            0, 0, 0x11, 1, 5, 0, 0, 0, 3, 0x20, 0x82, 0x87, 0x1, 0x8B, 0x9D, 0x29, 0xAC, 0x4B,
            0x8F, 0xA8, 0xE9, 0x19, 0x97, 0x21, 0xE9, 0x84,
        ])
        .read(&[0, 0, 1, 1, 5, 0, 0, 0, 3, 137])
        .build();

    let (mut client, mut conn) = client::handshake(io).await.expect("handshake");

    let req1 = client.get("https://http2.akamai.com");
    conn.drive(req1).await.expect("req1");

    let req2 = client.get("https://http2.akamai.com");
    conn.drive(req2).await.expect("req1");
}

#[tokio::test]
async fn server_settings_header_table_size() {
    // A client sets the SETTINGS_HEADER_TABLE_SIZE to 0, test that the
    // server doesn't send indexed headers.
    h2_support::trace_init!();

    let io = mock_io::Builder::new()
        .read(MAGIC_PREFACE)
        // Read SETTINGS_HEADER_TABLE_SIZE = 0
        .read(&[
            0, 0, 6, // len
            4, // type
            0, // flags
            0, 0, 0, 0, // stream id
            0, 0x1, // id = SETTINGS_HEADER_TABLE_SIZE
            0, 0, 0, 0, // value = 0
        ])
        .write(frames::SETTINGS)
        .write(frames::SETTINGS_ACK)
        .read(frames::SETTINGS_ACK)
        // Write GET /
        .read(&[
            0, 0, 0x10, 1, 5, 0, 0, 0, 1, 0x82, 0x87, 0x41, 0x8B, 0x9D, 0x29, 0xAC, 0x4B, 0x8F,
            0xA8, 0xE9, 0x19, 0x97, 0x21, 0xE9, 0x84,
        ])
        // Read response
        //.write(&[0, 0, 6, 1, 5, 0, 0, 0, 1, 136, 64, 129, 31, 129, 143])
        .write(&[0, 0, 7, 1, 5, 0, 0, 0, 1, 32, 136, 0, 129, 31, 129, 143])
        .build();

    let mut srv = server::handshake(io).await.expect("handshake");

    let (_req, mut stream) = srv.accept().await.unwrap().unwrap();

    let rsp = http::Response::builder()
        .status(200)
        .header("a", "b")
        .body(())
        .unwrap();
    stream.send_response(rsp, true).unwrap();

    assert!(srv.accept().await.is_none());
}

/// An I/O that keeps the bytes of every write call.
struct RecordWrites<T> {
    inner: T,
    writes: std::sync::Arc<std::sync::Mutex<Vec<Vec<u8>>>>,
}

impl<T: AsyncRead + Unpin> AsyncRead for RecordWrites<T> {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl<T: AsyncWrite + Unpin> AsyncWrite for RecordWrites<T> {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        let polled = std::pin::Pin::new(&mut self.inner).poll_write(cx, buf);
        if let std::task::Poll::Ready(Ok(n)) = polled {
            self.writes.lock().unwrap().push(buf[..n].to_vec());
        }
        polled
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

fn record_writes<T>(
    inner: T,
) -> (
    RecordWrites<T>,
    std::sync::Arc<std::sync::Mutex<Vec<Vec<u8>>>>,
) {
    let writes = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let io = RecordWrites {
        inner,
        writes: writes.clone(),
    };
    (io, writes)
}

/// (type, stream ID, payload length) of the frames in `bytes`, after a
/// connection preface at the start.
fn frame_heads(bytes: &[u8]) -> Vec<(u8, u32, usize)> {
    let mut rest = bytes.strip_prefix(&MAGIC_PREFACE[..]).unwrap_or(bytes);
    let mut out = Vec::new();
    while rest.len() >= 9 {
        let len = u32::from_be_bytes([0, rest[0], rest[1], rest[2]]) as usize;
        let stream = u32::from_be_bytes([rest[5], rest[6], rest[7], rest[8]]) & 0x7fff_ffff;
        out.push((rest[3], stream, len));
        rest = &rest[(9 + len).min(rest.len())..];
    }
    out
}

const DATA: u8 = 0;
const HEADERS: u8 = 1;
const SETTINGS: u8 = 4;
const WINDOW_UPDATE: u8 = 8;
const CONTINUATION: u8 = 9;

#[tokio::test]
async fn preface_alone_and_data_with_headers() {
    h2_support::trace_init!();
    let (io, mut srv) = mock::new();
    let (io, writes) = record_writes(io);

    let srv = async move {
        srv.read_preface().await.unwrap();
        let settings = srv.next().await.unwrap().unwrap();
        assert!(matches!(settings, frame::Frame::Settings(_)));
        srv.recv_frame(frames::window_update(0, (1 << 20) - 65_535))
            .await;
        srv.recv_frame(frames::headers(1).request("POST", "https://example.com/"))
            .await;
        srv.recv_frame(frames::data(1, &b"hello"[..]).eos()).await;
        srv.send_frame(frames::settings()).await;
        srv.recv_frame(frames::settings_ack()).await;
        srv.send_frame(frames::headers(1).response(200).eos()).await;
    };

    let client = async move {
        let (mut client, mut conn) = client::Builder::new()
            .write_frames_individually(true)
            .write_preface_alone(true)
            .write_data_with_headers(true)
            .initial_connection_window_size(1 << 20)
            .handshake::<_, Bytes>(io)
            .await
            .expect("handshake");
        // The preface went out in the handshake, on its own.
        assert_eq!(*writes.lock().unwrap(), vec![MAGIC_PREFACE.to_vec()]);

        let post = Request::post("https://example.com/").body(()).unwrap();
        let (response, mut stream) = client.send_request(post, false).unwrap();
        stream
            .send_data(Bytes::from_static(b"hello"), true)
            .unwrap();
        conn.drive(response).await.unwrap();

        let writes: Vec<Vec<(u8, u32)>> = writes.lock().unwrap()[1..]
            .iter()
            .map(|w| frame_heads(w).iter().map(|(t, s, _)| (*t, *s)).collect())
            .collect();
        assert_eq!(
            writes[..3],
            [
                vec![(SETTINGS, 0)],
                vec![(WINDOW_UPDATE, 0)],
                vec![(HEADERS, 1), (DATA, 1)],
            ]
        );
        drop(client);
        conn.await.unwrap();
    };

    join(srv, client).await;
}

#[tokio::test]
async fn frames_written_individually() {
    h2_support::trace_init!();
    let (io, mut srv) = mock::new();
    let (io, writes) = record_writes(io);

    let srv = async move {
        srv.read_preface().await.unwrap();
        let settings = srv.next().await.unwrap().unwrap();
        assert!(matches!(settings, frame::Frame::Settings(_)));
        srv.recv_frame(frames::window_update(0, (1 << 20) - 65_535))
            .await;
        for id in [1, 3] {
            srv.recv_frame(
                frames::headers(id)
                    .request("GET", "https://example.com/")
                    .eos(),
            )
            .await;
            srv.recv_frame(frames::window_update(id, (1 << 20) - 65_535))
                .await;
        }
        srv.send_frame(frames::settings()).await;
        srv.recv_frame(frames::settings_ack()).await;
        srv.send_frame(frames::headers(1).response(200).eos()).await;
        srv.send_frame(frames::headers(3).response(200).eos()).await;
    };

    let client = async move {
        let (mut client, mut conn) = client::Builder::new()
            .write_frames_individually(true)
            .initial_connection_window_size(1 << 20)
            .initial_stream_window_size(1 << 20)
            .handshake::<_, Bytes>(io)
            .await
            .expect("handshake");
        // Nothing is written before the connection is polled.
        assert!(writes.lock().unwrap().is_empty());

        let get = || Request::get("https://example.com/").body(()).unwrap();
        let (first, _) = client.send_request(get(), true).unwrap();
        let (second, _) = client.send_request(get(), true).unwrap();
        conn.drive(first).await.unwrap();
        conn.drive(second).await.unwrap();

        let writes: Vec<Vec<(u8, u32, usize)>> = writes
            .lock()
            .unwrap()
            .iter()
            .map(|w| frame_heads(w))
            .collect();
        assert_eq!(writes[0].len(), 2);
        assert_eq!((writes[0][0].0, writes[0][1].0), (SETTINGS, WINDOW_UPDATE));
        assert_eq!(
            writes[1..4]
                .iter()
                .map(|w| w.iter().map(|(t, s, _)| (*t, *s)).collect::<Vec<_>>())
                .collect::<Vec<_>>(),
            [
                vec![(HEADERS, 1), (WINDOW_UPDATE, 1)],
                vec![(HEADERS, 3), (WINDOW_UPDATE, 3)],
                vec![(SETTINGS, 0)],
            ]
        );
        drop(client);
        conn.await.unwrap();
    };

    join(srv, client).await;
}

#[tokio::test]
async fn header_block_split_at_max_header_frame_size() {
    h2_support::trace_init!();
    let (io, mut srv) = mock::new();
    let (io, writes) = record_writes(io);
    let (ready_tx, ready_rx) = futures::channel::oneshot::channel();

    let large = build_large_headers();
    let expected = large.iter().fold(
        frames::headers(1).request("GET", "https://example.com/"),
        |frame, &(name, ref value)| frame.field(name, &value[..]),
    );

    let srv = async move {
        let settings = frames::settings().max_frame_size(frame::MAX_MAX_FRAME_SIZE);
        srv.assert_client_handshake_with_settings(settings).await;
        ready_tx.send(()).unwrap();
        srv.recv_frame(expected.eos()).await;
        srv.send_frame(frames::headers(1).response(204).eos()).await;
    };

    let client = async move {
        let (mut client, mut conn) = client::Builder::new()
            .max_header_frame_size(16_384)
            .handshake::<_, Bytes>(io)
            .await
            .expect("handshake");
        conn.drive(ready_rx).await.unwrap();

        let mut request = Request::builder().uri("https://example.com/");
        for &(name, ref value) in &large {
            request = request.header(name, &value[..]);
        }
        let (response, _) = client
            .send_request(request.body(()).unwrap(), true)
            .unwrap();
        conn.drive(response).await.unwrap();

        let lengths: Vec<usize> = writes
            .lock()
            .unwrap()
            .iter()
            .flat_map(|w| frame_heads(w))
            .filter(|(kind, id, _)| (*kind == HEADERS || *kind == CONTINUATION) && *id == 1)
            .map(|(_, _, len)| len)
            .collect();
        assert!(lengths.len() > 2, "{:?}", lengths);
        let (last, full) = lengths.split_last().unwrap();
        assert!(full.iter().all(|len| *len == 16_384), "{:?}", lengths);
        assert!(*last <= 16_384);
        drop(client);
        conn.await.unwrap();
    };

    join(srv, client).await;
}
