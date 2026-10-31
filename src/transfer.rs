use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering::Relaxed},
    },
    task::Poll::{self, Ready},
};

use futures::task::AtomicWaker;
use ringbuf::{
    SharedRb,
    storage::Heap,
    traits::{Consumer, Producer, Split},
    wrap::caching::Caching,
};

pub struct Reader {
    pub inner: Caching<Arc<SharedRb<Heap<u8>>>, false, true>,
    recvwaker: Arc<AtomicWaker>,
    sendwaker: Arc<AtomicWaker>,
    closed: Arc<AtomicBool>,
}

impl Reader {
    pub fn pop_slice(&mut self, cx: &mut std::task::Context<'_>, elems: &mut [u8]) -> Poll<usize> {
        let n = self.inner.pop_slice(elems);
        if n == 0 {
            if self.closed.load(Relaxed) {
                return Ready(0);
            }
            self.recvwaker.register(cx.waker());
            self.wake();
            let n = self.inner.pop_slice(elems);
            if n == 0 {
                return Poll::Pending;
            }
        }

        Ready(n)
    }

    pub fn wake(&self) {
        self.sendwaker.wake();
    }
}

impl Drop for Reader {
    fn drop(&mut self) {
        self.inner.clear();
        self.closed.store(true, Relaxed);
    }
}

pub struct Writer {
    pub inner: Caching<Arc<SharedRb<Heap<u8>>>, true, false>,
    recvwaker: Arc<AtomicWaker>,
    sendwaker: Arc<AtomicWaker>,
    pub closed: Arc<AtomicBool>,
}

impl Writer {
    pub fn push_slice(&mut self, cx: &mut std::task::Context<'_>, elems: &[u8]) -> Poll<usize> {
        if self.closed.load(Relaxed) {
            self.wake();
            return Ready(0);
        }
        let n = self.inner.push_slice(elems);
        if n == 0 {
            self.sendwaker.register(cx.waker());
            self.wake();
            let n = self.inner.push_slice(elems);
            if n == 0 {
                return Poll::Pending;
            }
        }
        Ready(n)
    }

    pub fn wake(&self) {
        self.recvwaker.wake();
    }
}

impl Drop for Writer {
    fn drop(&mut self) {
        self.closed.store(true, Relaxed);
    }
}

/// heap_size need != 0 && < usize::Max / 2
pub fn transfer(heap_size: usize) -> (Writer, Reader) {
    //0x3FFF * 20
    let rb = ringbuf::HeapRb::new(heap_size);
    let (w, r) = rb.split();
    let recvwaker = Arc::new(AtomicWaker::new());
    let sendwaker = Arc::new(AtomicWaker::new());
    let closed = Arc::new(AtomicBool::new(false));
    (
        Writer {
            inner: w,
            recvwaker: recvwaker.clone(),
            sendwaker: sendwaker.clone(),
            closed: closed.clone(),
        },
        Reader {
            inner: r,
            recvwaker,
            sendwaker,
            closed,
        },
    )
}
