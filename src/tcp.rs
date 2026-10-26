use std::{
    collections::HashMap, net::{Ipv4Addr, Ipv6Addr, SocketAddr}, pin::{Pin, pin}, task::{
        Poll::{Pending, Ready},
        ready,
    }
};

use bytes::Bytes;
use futures::{
    AsyncRead, AsyncWrite, FutureExt, Stream, channel::mpsc::{Receiver, SendError, Sender,},
};
use futures_timer::Delay;
use ringbuf::traits::{Consumer, Observer, Producer as _};
use smoltcp::{
    iface::{Config, Interface, PollResult, SocketHandle, SocketSet}, phy::Device, socket::tcp::{SocketBuffer}, time::{Duration, Instant}, wire::{HardwareAddress, IpAddress, IpCidr, TcpPacket},
};

use crate::{device::VirtualDevice, stack::IpPacket, transfer::{self, Reader, Writer}};

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
    packet_cache: Option<(Bytes, Option<(std::net::SocketAddr, std::net::SocketAddr)>)>,
    iface: Iface,
    sockets: HashMap<SocketHandle, (Writer, Reader)>,
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
        &mut HashMap<SocketHandle, (Writer, Reader)>,
        &mut SocketSet<'static>,
        &mut Vec<SocketHandle>
    ) {
        return (&mut self.sockets, &mut self.iface.socketset, &mut self.removesocket);
    }

    fn smoltcp_poll(
        &mut self,
        cx: &mut std::task::Context<'_>,
        packet: Bytes,
        mut initdata: Option<(SocketAddr, SocketAddr)>,
    ) -> std::task::Poll<Result<(TcpStream, SocketAddr, SocketAddr), SendError>> {
        match self.smoltcp_tx.poll_ready(cx) {
            Ready(Err(e)) => {
                return Ready(Err(e));
            }
            Ready(Ok(())) => {
                cx.waker().wake_by_ref();
                self.smoltcp_tx.start_send(packet)?;
                if let Some((src, dst)) = initdata.take() {
                    let mut socket = smoltcp::socket::tcp::Socket::new(
                        SocketBuffer::new(vec![0u8; self.tcp_buffer_size]),
                        SocketBuffer::new(vec![0u8; self.tcp_buffer_size]),
                    );
                    socket.set_keep_alive(Some(Duration::from_secs(28)));
                    socket.set_timeout(Some(Duration::from_secs(7200)));
                    socket.listen(dst).unwrap();


                    let (streamprob, streamcons) = transfer::transfer(0x3FFF * 20);
                    let (socketprob, socketcons) = transfer::transfer(0x3FFF * 20);

                    let handle = self.iface.socketset.add(socket);
                    self.sockets.insert(handle, (socketprob, streamcons));
                    let stream = TcpStream::new(streamprob, socketcons);
                    return Ready(Ok((stream, src, dst)))
                } else {
                    return Pending;
                }
                
            }
            Pending => {
                self.packet_cache.replace((packet, initdata));
                return Pending;
            }
        };
    }

    fn iface_poll(&mut self) {
        let timestamp = Instant::now();
        self.iface.poll(timestamp);
        let (sockets, socketset, removesockets) = self.rebain();
        
        for (handle, (send_tx, recv_rx)) in sockets.iter_mut() {
            let socket: &mut smoltcp::socket::tcp::Socket = socketset.get_mut(*handle);
            // if socket.state() == State::Closed {
            //     recv_rx.close();
            //     send_tx.close_channel();
            //     removesockets.push(*handle);
            //     continue;
            // }
            // if send_tx.is_closed() {
            //     socket.abort();
            //     continue;
            // }

            if socket.can_recv() {
                if let Err(_) = socket.recv(|buf| {
                    let n = send_tx.inner.push_slice(buf);
                    send_tx.wake();
                    (n, ())
                }) {
                    socket.abort();
                    //send_tx.close_channel();
                    continue;
                }
            }

            while socket.can_send() && !recv_rx.inner.is_empty() {
                if let Err(_) = socket.send(|buf| {
                    let n = recv_rx.inner.pop_slice(buf);
                    recv_rx.wake();
                    (n, ())

                }) {
                    socket.abort();
                    //recv_rx.close();
                    continue;
                }
            }
        }

        while let Some(handle) = self.removesocket.pop() {
            self.sockets.remove(&handle);
            self.iface.socketset.remove(handle);
        }
        let du = self.poll_delay(timestamp);
        self.polled.reset(du.into());
    }

    pub fn poll_delay(&mut self, stamp: Instant) -> Duration {
        self.iface.iface.poll_delay(stamp, &self.iface.socketset).unwrap_or(Duration::from_millis(5)).min(Duration::from_millis(300))
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

        let initdata = if initpacket {
            let srcaddr = SocketAddr::new(src_addr, packet.src_port());
            let dstaddr = SocketAddr::new(dst_addr, packet.dst_port());
            Some((srcaddr, dstaddr))
        } else { None };

        match self.smoltcp_poll(cx, frame, initdata) {
            Ready(Err(_)) => return Ready(None),
            Ready(Ok(initdata)) => {
                Ready(Some(initdata))
            }
            Pending => return Pending,
        }
    }
}

impl Stream for TcpListener {
    type Item = (TcpStream, SocketAddr, SocketAddr);

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        if let Some((packet, initdata)) = self.packet_cache.take() {
            match self.smoltcp_poll(cx, packet, initdata) {
                Ready(Err(_)) => return Ready(None),
                Ready(Ok(x)) => { return Ready(Some(x)) }
                Pending => {  },
            };
        }
        match self.accept_stream(cx) {
            Pending => {}
            Ready(a) => {
                return Ready(a);
            }
        }
        
        
        match self.polled.poll_unpin(cx) {
            Pending => { }
            Ready(()) => {
                self.iface_poll();
                cx.waker().wake_by_ref();

            }
        }
        
        Pending
    }
}

// next函数将数据塞入recv_buffer, 用户态将数据塞入send_buffer
pub struct TcpStream {
    send_ch: Pin<Box<Writer>>,
    recv_ch: Pin<Box<Reader>>,
}

impl TcpStream {
    pub fn new(send_ch: Writer, recv_ch: Reader) -> Self {
        TcpStream { send_ch: Box::pin(send_ch), recv_ch: Box::pin(recv_ch) }
    }
}

impl AsyncRead for TcpStream {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut [u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        let n = ready!(self.recv_ch.pop_slice(cx, buf));
        Ready(Ok(n))
    }
}

impl AsyncWrite for TcpStream {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        let n = ready!(self.send_ch.push_slice(cx, buf));
        Ready(Ok(n))
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        Ready(Ok(()))
        // Ready(
        //     ready!(self.send_ch.as_mut().poll_flush(cx))
        //         .map_err(|_| std::io::ErrorKind::BrokenPipe.into()),
        // )
    }

    fn poll_close(
        mut self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        // self.send_ch.close_channel();
        Ready(Ok(()))
    }
}
