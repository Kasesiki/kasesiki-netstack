use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};

use futures::{SinkExt, StreamExt};
use netstack_smoltcp::stack::StackBuilder;
use tokio::{io::copy_bidirectional, net::{TcpSocket, TcpStream}};
use tokio_util::compat::FuturesAsyncReadCompatExt as _;


#[tokio::main]
async fn main() -> std::io::Result<()> {
    let (_stack, mut _tcplistener) = StackBuilder::default().build()?;

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

    let (mut stackr, mut stackw) = _stack.split();
    
    let a = tokio::spawn(async move {
        while let Some(Ok(buf)) = tun_stream.next().await {
            if let Err(err) = stackr.send(buf.into()).await {
                println!("{err}")
            }
        }
    });

    let b = tokio::spawn(async move {
        while let Some(byte) = stackw.next().await {
            tun_sink.send(byte.into()).await.unwrap();
        }
    });

    tokio::spawn(async move {
        while let Some((stream, src, dst)) = _tcplistener.next().await {
            tokio::spawn(async move {
                println!("new tcp connection: {:?} => {:?}", src, dst);
                match new_tcp_stream(dst).await {
                    Ok(mut remote_stream) => {
                        let _ = copy_bidirectional(&mut stream.compat(), &mut remote_stream).await;
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