use std::{net::{Ipv4Addr, SocketAddr, SocketAddrV4}, pin::Pin, sync::Arc};

use futures::{SinkExt, StreamExt, task::AtomicWaker};
use netstack_smoltcp::{stack::StackBuilder, tcp};
use tokio::{io::copy_bidirectional, net::{TcpSocket, TcpStream}, time::{Instant, Sleep, sleep}};

pub struct Timer {
    inner: Pin<Box<Sleep>>,
}

impl tcp::Timer for Timer {
    fn reset(&mut self, dur: netstack_smoltcp::Duration) {
        self.inner.as_mut().reset(Instant::now() + dur.into());
    }
}

impl Future for Timer {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Self::Output> {
        let a = unsafe { self.map_unchecked_mut(|f| { &mut f.inner }) };
        a.poll(cx)
    }
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let waker = Arc::new(AtomicWaker::new());
    let s = sleep(Instant::now().elapsed());

    let (stack, mut _tcplistener) = StackBuilder::default().build(waker.clone(), Timer { inner: Box::pin(s) })?;

    let mut cfg = tun2::Configuration::default();
    cfg.layer(tun2::Layer::L3);
    let fd = -1;
    if fd >= 0 {
        cfg.raw_fd(fd);
    } else {
        cfg.tun_name("tun7")
            .address("10.10.10.2")
            .destination("10.10.10.1")
            .mtu(tun2::DEFAULT_MTU);
        #[cfg(not(any(target_arch = "mips", target_arch = "mips64",)))]
        {
            cfg.netmask("255.255.255.0");
        }
        cfg.up();
    }

    let device = tun2::create_as_async(&cfg).unwrap();
    let framed = device.into_framed();
    let (mut tun_sink, mut tun_stream) = framed.split();

    let (mut stackr, mut stackw) = stack.split();
    let a = tokio::spawn(async move {
        while let Some(Ok(buf)) = tun_stream.next().await {
            if let Err(err) = stackr.send(buf.into()).await {
                println!("{err}")
            }
            waker.wake();
        }
    });

    let b = tokio::spawn(async move {
        while let Some(byte) = stackw.next().await {
            tun_sink.send(byte.into()).await.unwrap();
        }
    });

    tokio::spawn(async move {
        while let Some((mut stream, src, dst)) = _tcplistener.next().await {
            tokio::spawn(async move {
                println!("new tcp connection: {:?} => {:?}", src, dst);
                match new_tcp_stream(dst).await {
                    Ok(mut remote_stream) => {
                        let _ = copy_bidirectional(&mut stream, &mut remote_stream).await;
                    }
                    Err(e) => println!(
                        "failed to new tcp stream {:?}=>{:?}, err: {:?}",
                        src, dst, e
                    ),
                }
            });
        }
    });

    let _ = tokio::join!(a, b);

    Ok(())
    
}

async fn new_tcp_stream<'a>(addr: SocketAddr) -> std::io::Result<TcpStream> {
    let socket = socket2::Socket::new(socket2::Domain::IPV4, socket2::Type::STREAM, None)?;
    socket.set_keepalive(true)?;
    socket.set_tcp_nodelay(true)?;
    socket.set_nonblocking(true)?;

    let stream = TcpSocket::from_std_stream(socket.into())
        .connect(SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), addr.port())))
        .await?;

    Ok(stream)
}