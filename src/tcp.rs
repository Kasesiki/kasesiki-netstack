use std::{
    collections::HashMap, net::{Ipv4Addr, Ipv6Addr, SocketAddr}, pin::{Pin, pin}, task::{
        Poll::{Pending, Ready},
        ready,
    }
};

use bytes::Bytes;
use futures::{
    AsyncRead, AsyncWrite, FutureExt, Sink, Stream, channel::mpsc::{Receiver, SendError, Sender, channel},
};
use futures_timer::Delay;
use smoltcp::{
    iface::{Config, Interface, PollResult, SocketHandle, SocketSet}, phy::Device, socket::tcp::{SocketBuffer, State}, time::{Duration, Instant}, wire::{HardwareAddress, IpAddress, IpCidr, TcpPacket},
};

use crate::{device::VirtualDevice, stack::IpPacket};

struct Iface {
    pub iface: Interface,
    device: VirtualDevice,
    pub socketset: SocketSet<'static>,
}

impl Iface {
    pub fn poll(&mut self, instant: Instant) -> PollResult {
        self.iface
            .poll(instant, &mut self.device, &mut self.socketset)
    }
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

pub trait Timer {
    
}

pub struct TcpListener {
    tcp_rx: Receiver<IpPacket<bytes::Bytes>>,
    smoltcp_tx: Sender<Bytes>,
    tcp_buffer_size: usize,
    packet_cache: Option<Bytes>,
    iface: Iface,
    sockets: HashMap<SocketHandle, (Sender<Vec<u8>>, Receiver<Vec<u8>>)>,
    removesocket: Vec<SocketHandle>,
    polled: futures_timer::Delay,
}

impl TcpListener {
    pub fn new(
        mut device: VirtualDevice,
        tcp_rx: Receiver<IpPacket<Bytes>>,
        tcp_buffer_size: usize,
        smoltcp_tx: Sender<Bytes>,
    ) -> std::io::Result<Self> {
        let iface = create_interface(&mut device)?;
        let socketset = SocketSet::new(vec![]);
        Ok(TcpListener {
            tcp_rx,
            tcp_buffer_size,
            smoltcp_tx,
            packet_cache: None,
            iface: Iface {
                iface,
                device,
                socketset,
            },
            polled: Delay::new(std::time::Duration::from_millis(1)),
            sockets: HashMap::new(),
            removesocket: vec![],
        })
    }

    pub fn rebain(
        &mut self,
    ) -> (
        &mut HashMap<SocketHandle, (Sender<Vec<u8>>, Receiver<Vec<u8>>)>,
        &mut SocketSet<'static>,
        &mut Vec<SocketHandle>
    ) {
        return (&mut self.sockets, &mut self.iface.socketset, &mut self.removesocket);
    }

    fn smoltcp_poll(
        &mut self,
        cx: &mut std::task::Context<'_>,
        packet: Bytes,
    ) -> std::task::Poll<Result<(), SendError>> {
        match self.smoltcp_tx.poll_ready(cx) {
            Ready(Err(e)) => {
                return Ready(Err(e));
            }
            Ready(Ok(())) => {
                return Ready(self.smoltcp_tx.start_send(packet));
            }
            Pending => {
                self.packet_cache.replace(packet);
                return Pending;
            }
        };
    }

    fn iface_poll(&mut self, timestamp: Instant) {
        self.iface.poll(timestamp);
        let (sockets, socketset, removesockets) = self.rebain();
        
        for (handle, (send_tx, recv_rx)) in sockets.iter_mut() {
            let socket: &mut smoltcp::socket::tcp::Socket = socketset.get_mut(*handle);
            if socket.state() == State::Closed {
                recv_rx.close();
                send_tx.close_channel();
                removesockets.push(*handle);
                continue;
            }
            if send_tx.is_closed() {
                socket.abort();
                continue;
            }

            if socket.can_recv() {
                if let Err(_) = socket.recv(|buf| {
                    if let Err(_) = send_tx.try_send(buf.to_vec()) {
                        (buf.len(), ())
                    } else {
                        (0, ())
                    }
                }) {
                    socket.abort();
                    send_tx.close_channel();
                    continue;
                }
            }

            if socket.can_send() && let Ok(data) = recv_rx.try_recv() {
                if let Err(_) = socket.send(|buf| {
                    buf.copy_from_slice(&data);
                    (data.len(), ())
                }) {
                    socket.abort();
                    recv_rx.close();
                    continue;
                }
            }
        }

        while let Some(handle) = self.removesocket.pop() {
            self.sockets.remove(&handle);
            self.iface.socketset.remove(handle);
        }
        let du = self.poll_delay(timestamp).into();
        self.polled.reset(du);
    }

    pub fn poll_delay(&mut self, stamp: Instant) -> Duration {
        self.iface.iface.poll_delay(stamp, &self.iface.socketset).unwrap_or(Duration::from_millis(5))
    }

    fn accept_stream(&mut self, cx: &mut std::task::Context<'_>) -> std::task::Poll<Option<(TcpStream, SocketAddr, SocketAddr)>> {

        let Ok(a) = ready!(pin!(self.tcp_rx.recv()).poll(cx)) else {
            panic!()
        };

        let src_addr = a.src_addr();
        let dst_addr = a.dst_addr();

        let range = a.size();
        let frame = a.take();

        let Ok(packet) = TcpPacket::new_checked(&frame[range]) else {
            panic!()
        };
        let initpacket = packet.syn() && !packet.ack();
        let srcaddr = SocketAddr::new(src_addr, packet.src_port());
        let dstaddr = SocketAddr::new(dst_addr, packet.dst_port());

        match self.smoltcp_poll(cx, frame) {
            Ready(Err(_)) => return Ready(None),
            Ready(Ok(())) => {
                return Ready(if initpacket {
                    
                    let mut socket = smoltcp::socket::tcp::Socket::new(
                        SocketBuffer::new(vec![0u8; self.tcp_buffer_size]),
                        SocketBuffer::new(vec![0u8; self.tcp_buffer_size]),
                    );

                    socket.set_keep_alive(Some(Duration::from_secs(28)));
                    socket.set_timeout(Some(Duration::from_secs(7200)));

                    socket.listen(dstaddr).unwrap();
                    let (recv_tx, recv_rx) = channel(16);
                    let (send_tx, send_rx) = channel(16);
                    let handle = self.iface.socketset.add(socket);
                    self.sockets.insert(handle, (recv_tx, send_rx));
                    let stream = TcpStream::new(send_tx, recv_rx);
                    Some((stream, srcaddr, dstaddr))
                } else {
                    cx.waker().wake_by_ref();
                    return Pending
                });
            }
            Pending => return Pending,
        };
    }
}

impl Stream for TcpListener {
    type Item = (TcpStream, SocketAddr, SocketAddr);

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        if let Some(packet) = self.packet_cache.take() {
            match self.smoltcp_poll(cx, packet) {
                Ready(Err(_)) => return Ready(None),
                Ready(Ok(())) => {}
                Pending => return Pending,
            };
        }
        let stamp = Instant::now();
        match self.polled.poll_unpin(cx) {
            Pending => {}
            Ready(()) => {
                self.iface_poll(stamp);
            }
        }

        self.accept_stream(cx)
    }
}

// next函数将数据塞入recv_buffer, 用户态将数据塞入send_buffer
pub struct TcpStream {
    send_ch: Pin<Box<Sender<Vec<u8>>>>,
    recv_ch: Pin<Box<Receiver<Vec<u8>>>>,
}

impl TcpStream {
    pub fn new(send_ch: Sender<Vec<u8>>, recv_ch: Receiver<Vec<u8>>) -> Self {
        TcpStream { send_ch: Box::pin(send_ch), recv_ch: Box::pin(recv_ch) }
    }
}

impl AsyncRead for TcpStream {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut [u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        match self.recv_ch.as_mut().poll_next(cx) {
            Pending => Pending,
            Ready(Some(byte)) => {
                if byte.len() > buf.len() {
                    buf.copy_from_slice(&byte[..buf.len()]);
                    Ready(Ok(buf.len()))
                } else {
                    buf[..byte.len()].copy_from_slice(&byte);
                    Ready(Ok(byte.len()))
                }

            }
            Ready(None) => Ready(Err(std::io::ErrorKind::BrokenPipe.into())),
        }
    }
}

impl AsyncWrite for TcpStream {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        if let Err(_) = ready!(self.send_ch.poll_ready(cx)) {
            return Ready(Err(std::io::ErrorKind::BrokenPipe.into()));
        }
        let _ = self.send_ch.start_send(buf.to_vec());
        let _ = ready!(self.send_ch.as_mut().poll_flush(cx));

        Ready(Ok(buf.len()))
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        Ready(
            ready!(self.send_ch.as_mut().poll_flush(cx))
                .map_err(|_| std::io::ErrorKind::BrokenPipe.into()),
        )
    }

    fn poll_close(
        mut self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        self.send_ch.close_channel();
        Ready(Ok(()))
    }
}
