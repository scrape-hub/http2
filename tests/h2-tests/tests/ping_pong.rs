use futures::channel::oneshot;
use futures::StreamExt;
use h2_support::assert_ping;
use h2_support::prelude::*;

#[tokio::test]
async fn recv_single_ping() {
    h2_support::trace_init!();
    let (m, mut mock) = mock::new();

    // Create the handshake
    let h2 = async move {
        let (_client, conn) = client::handshake(m).await.unwrap();
        let _: () = conn.await.unwrap();
    };

    let mock = async move {
        let _ = mock.assert_client_handshake().await;
        let frame = frame::Ping::new(Default::default());
        mock.send(frame.into()).await.unwrap();
        let frame = mock.next().await.unwrap();

        let pong = assert_ping!(frame.unwrap());

        // Payload is correct
        assert_eq!(*pong.payload(), <[u8; 8]>::default());

        // Is ACK
        assert!(pong.is_ack());
    };

    join(mock, h2).await;
}

#[tokio::test]
async fn recv_multiple_pings() {
    h2_support::trace_init!();
    let (io, mut client) = mock::new();

    let client = async move {
        let settings = client.assert_server_handshake().await;
        assert_default_settings!(settings);
        client.send_frame(frames::ping([1; 8])).await;
        client.send_frame(frames::ping([2; 8])).await;
        client.recv_frame(frames::ping([1; 8]).pong()).await;
        client.recv_frame(frames::ping([2; 8]).pong()).await;
    };

    let srv = async move {
        let mut s = server::handshake(io).await.expect("handshake");
        assert!(s.next().await.is_none());
    };

    join(client, srv).await;
}

#[tokio::test]
async fn pong_has_highest_priority() {
    h2_support::trace_init!();
    let (io, mut client) = mock::new();

    let data = Bytes::from(vec![0; 16_384]);
    let data_clone = data.clone();

    let client = async move {
        let settings = client.assert_server_handshake().await;
        assert_default_settings!(settings);
        client
            .send_frame(frames::headers(1).request("POST", "https://http2.akamai.com/"))
            .await;
        client.send_frame(frames::data(1, data_clone).eos()).await;
        client.send_frame(frames::ping([1; 8])).await;
        client.recv_frame(frames::ping([1; 8]).pong()).await;
        client
            .recv_frame(frames::headers(1).response(200).eos())
            .await;
    };

    let srv = async move {
        let mut s = server::handshake(io).await.expect("handshake");
        let (req, mut stream) = s.next().await.unwrap().unwrap();
        assert_eq!(req.method(), "POST");
        let body = req.into_parts().1;

        let body = util::concat(body).await.expect("body");
        assert_eq!(body.len(), data.len());
        let res = Response::builder().status(200).body(()).unwrap();
        stream.send_response(res, true).expect("response");
        assert!(s.next().await.is_none());
    };

    join(client, srv).await;
}

#[tokio::test]
async fn user_ping_pong() {
    h2_support::trace_init!();
    let (io, mut srv) = mock::new();

    let srv = async move {
        let settings = srv.assert_client_handshake().await;
        assert_default_settings!(settings);
        srv.recv_frame(frames::ping(frame::Ping::USER)).await;
        srv.send_frame(frames::ping(frame::Ping::USER).pong()).await;
        srv.recv_frame(frames::go_away(0)).await;
        srv.recv_eof().await;
    };

    let client = async move {
        let (client, mut conn) = client::handshake(io).await.expect("client handshake");
        // yield once so we can ack server settings
        conn.drive(util::yield_once()).await;
        // `ping_pong()` method conflict with mock future ext trait.
        let mut ping_pong = client::Connection::ping_pong(&mut conn).expect("taking ping_pong");
        ping_pong.send_ping(Ping::opaque()).expect("send ping");

        // multiple pings results in a user error...
        assert_eq!(
            ping_pong
                .send_ping(Ping::opaque())
                .expect_err("ping 2")
                .to_string(),
            "user error: send_ping before received previous pong",
            "send_ping while ping pending is a user error",
        );

        conn.drive(futures::future::poll_fn(move |cx| ping_pong.poll_pong(cx)))
            .await
            .unwrap();
        drop(client);
        conn.await.expect("client");
    };

    join(srv, client).await;
}

#[tokio::test]
async fn user_notifies_when_connection_closes() {
    h2_support::trace_init!();
    let (io, mut srv) = mock::new();
    let srv = async move {
        let settings = srv.assert_client_handshake().await;
        assert_default_settings!(settings);
        srv
    };

    #[allow(clippy::async_yields_async)]
    let client = async move {
        let (_client, mut conn) = client::handshake(io).await.expect("client handshake");
        // yield once so we can ack server settings
        conn.drive(util::yield_once()).await;
        conn
    };

    let (srv, mut client) = join(srv, client).await;

    // `ping_pong()` method conflict with mock future ext trait.
    let mut ping_pong = client::Connection::ping_pong(&mut client).expect("taking ping_pong");

    // Spawn a thread so we can park a task waiting on `poll_pong`, and then
    // drop the client and be sure the parked task is notified...
    let (tx, rx) = oneshot::channel();
    tokio::spawn(async move {
        poll_fn(|cx| ping_pong.poll_pong(cx))
            .await
            .expect_err("poll_pong should error");
        tx.send(ping_pong).unwrap();
    });

    // Sleep to let the ping thread park its task...
    idle_ms(50).await;
    drop(client);
    drop(srv);

    let mut ping_pong = rx.await.expect("ping pong spawn join");

    // Now that the connection is closed, also test `send_ping` errors...
    assert_eq!(
        ping_pong
            .send_ping(Ping::opaque())
            .expect_err("send_ping")
            .to_string(),
        "broken pipe",
    );
}

#[tokio::test]
async fn user_ping_with_payload() {
    h2_support::trace_init!();
    let (io, mut srv) = mock::new();
    let second = [0, 0, 0, 0, 0, 0, 0, 2];
    let (ready_tx, ready_rx) = oneshot::channel();

    let srv = async move {
        let settings = srv.assert_client_handshake().await;
        assert_default_settings!(settings);
        ready_tx.send(()).unwrap();
        srv.recv_frame(frames::ping([0; 8])).await;
        srv.send_frame(frames::ping([0; 8]).pong()).await;
        srv.recv_frame(frames::ping(second)).await;
        srv.send_frame(frames::ping(second).pong()).await;
    };

    let client = async move {
        let (client, mut conn) = client::handshake(io).await.expect("handshake");
        let mut ping_pong = client::Connection::ping_pong(&mut conn).expect("taking ping_pong");
        conn.drive(ready_rx).await.unwrap();
        let pings = async move {
            ping_pong.ping(Ping::with_payload([0; 8])).await.unwrap();
            ping_pong.ping(Ping::with_payload(second)).await.unwrap();
        };
        conn.drive(pings).await;
        drop(client);
        conn.await.unwrap();
    };

    join(srv, client).await;
}

#[tokio::test]
async fn ping_from_request_follows_its_headers() {
    h2_support::trace_init!();
    let (io, mut srv) = mock::new();
    let payload = [0, 0, 0, 0, 0, 0, 0, 1];

    let srv = async move {
        let settings = srv.assert_client_handshake().await;
        assert_default_settings!(settings);
        srv.recv_frame(
            frames::headers(1)
                .request("GET", "https://example.com/")
                .eos(),
        )
        .await;
        srv.recv_frame(frames::ping(payload)).await;
        srv.send_frame(frames::ping(payload).pong()).await;
        srv.send_frame(frames::headers(1).response(200).eos()).await;
        // Only the request that carried it sends a PING.
        srv.recv_frame(
            frames::headers(3)
                .request("GET", "https://example.com/")
                .eos(),
        )
        .await;
        srv.send_frame(frames::headers(3).response(200).eos()).await;
    };

    let client = async move {
        let (mut client, mut conn) = client::handshake(io).await.expect("handshake");
        let request = Request::builder()
            .uri("https://example.com/")
            .extension(frame::Ping::new(payload))
            .body(())
            .unwrap();
        let (response, _) = client.send_request(request, true).unwrap();
        conn.drive(response).await.unwrap();
        let (response, _) = client
            .send_request(Request::get("https://example.com/").body(()).unwrap(), true)
            .unwrap();
        conn.drive(response).await.unwrap();
        drop(client);
        conn.await.unwrap();
    };

    join(srv, client).await;
}
