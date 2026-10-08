use std::{net::{Ipv4Addr, Ipv6Addr}, pin::pin, task::{Poll::{Pending, Ready}, ready}};

use bytes::Bytes;
use futures::{AsyncRead, AsyncWrite, Stream, channel::mpsc::{Receiver, SendError, Sender}};
use smoltcp::{iface::{Config, Interface}, phy::Device, socket::tcp::SocketBuffer, time::{Duration, Instant}, wire::{HardwareAddress, IpAddress, IpCidr, TcpPacket}};

use crate::{device::VirtualDevice, stack::IpPacket};

pub struct TcpListener {
    tcp_rx: Receiver<IpPacket<bytes::Bytes>>,
    smoltcp_tx: Sender<Bytes>,
    tcp_buffer_size: usize,
    packet_cache: Option<smoltcp::wire::TcpPacket<Bytes>>,
}

fn create_interface<D>(device: &mut D) -> std::io::Result<Interface>
where
    D: Device + ?Sized,
{
    let mut iface_config = Config::new(HardwareAddress::Ip);
    iface_config.random_seed = Instant::now().total_millis().unsigned_abs();
    let mut iface = Interface::new(iface_config, device, Instant::now());
    iface.update_ip_addrs(|ip_addrs| {
        ip_addrs
            .push(IpCidr::new(IpAddress::v4(0, 0, 0, 1), 0))
            .expect("iface IPv4");
        ip_addrs
            .push(IpCidr::new(IpAddress::v6(0, 0, 0, 0, 0, 0, 0, 1), 0))
            .expect("iface IPv6");
    });
    iface
        .routes_mut()
        .add_default_ipv4_route(Ipv4Addr::new(0, 0, 0, 1))
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::AddrNotAvailable, e))?;
    iface
        .routes_mut()
        .add_default_ipv6_route(Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, 1))
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::AddrNotAvailable, e))?;
    iface.set_any_ip(true);
    Ok(iface)
}

impl TcpListener {
    pub fn new(mut device: VirtualDevice, tcp_rx: Receiver<IpPacket<Bytes>>, tcp_buffer_size: usize, smoltcp_tx: Sender<Bytes>) -> std::io::Result<Self> {
        let iface = create_interface(&mut device)?;

        Ok(TcpListener { tcp_rx, tcp_buffer_size, smoltcp_tx, packet_cache: None })
    }

    fn smoltcp_poll(&mut self, cx: &mut std::task::Context<'_>, packet: smoltcp::wire::TcpPacket<Bytes>) -> std::task::Poll<Result<(), SendError>> {
        match self.smoltcp_tx.poll_ready(cx) {
            Ready(Err(e)) => {
                return Ready(Err(e));
            },
            Ready(Ok(())) => {
                return Ready(self.smoltcp_tx.start_send(packet.into_inner()));
            },
            Pending => {
                self.packet_cache.replace(packet);
                return Pending
            }
        };
    }
}

impl Stream for TcpListener {
    type Item = ();

    fn poll_next(mut self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Option<Self::Item>> {
        if let Some(packet) = self.packet_cache.take() {
            match self.smoltcp_poll(cx, packet) {
                Ready(Err(_)) => {
                    return Ready(None)
                },
                Ready(Ok(())) => {},
                Pending => {return Pending}
            };
        }   

        let Ok(a) = ready!(pin!(self.tcp_rx.recv()).poll(cx)) else { panic!() };

        let dst_addr = a.dst_addr();

        let Ok(packet) = TcpPacket::new_checked(a.take()) else { panic!() };

        if packet.syn() && !packet.ack() {
            let mut socket = smoltcp::socket::tcp::Socket::new(SocketBuffer::new(vec![0u8; self.tcp_buffer_size]), SocketBuffer::new(vec![0u8; self.tcp_buffer_size]));

            socket.set_keep_alive(Some(Duration::from_secs(28)));
            socket.set_timeout(Some(Duration::from_secs(7200)));

            socket.listen((dst_addr, packet.dst_port())).unwrap();
        }

        match self.smoltcp_poll(cx, packet) {
            Ready(Err(_)) => {
                return Ready(None)
            },
            Ready(Ok(())) => {},
            Pending => {return Pending}
        };
        
        todo!()
    }
}

pub struct TcpStream {

}

impl AsyncRead for TcpStream {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut [u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        todo!()
    }
}

impl AsyncWrite for TcpStream {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        todo!()
    }

    fn poll_flush(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<std::io::Result<()>> {
        todo!()
    }

    fn poll_close(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<std::io::Result<()>> {
        todo!()
    }
}