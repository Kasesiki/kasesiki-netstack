use futures::{SinkExt, StreamExt};
use netstack_smoltcp::stack::StackBuilder;


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
        while let Some((_stream, src, dst)) = _tcplistener.next().await {
            println!("{src} - {dst}");
        }
        println!("tcpover.")
    });

    let _ = tokio::join!(a, b);

    Ok(())
    
}
