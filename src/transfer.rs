use std::{sync::Arc, task::Poll::{self, Ready}};

use futures::task::AtomicWaker;
use ringbuf::{SharedRb, storage::Heap, traits::{Consumer, Producer, Split}, wrap::caching::Caching};

pub struct Reader {
    pub inner: Caching<Arc<SharedRb<Heap<u8>>>, false, true>,
    recvwaker: Arc<AtomicWaker>,
    sendwaker: Arc<AtomicWaker>,
}

impl Reader {
    pub fn pop_slice(&mut self, cx: &mut std::task::Context<'_>, elems: &mut [u8]) -> Poll<usize> {
        let n = self.inner.pop_slice(elems);
        if n == 0 {
            self.recvwaker.register(cx.waker());
            let n = self.inner.pop_slice(elems);
            if n == 0 {
                return Poll::Pending;
            }
        }
        self.wake();
        Ready(n)
    }

    pub fn wake(&self) {
        self.sendwaker.wake();
    }
}

pub struct Writer {
    pub inner: Caching<Arc<SharedRb<Heap<u8>>>, true, false>,
    recvwaker: Arc<AtomicWaker>,
    sendwaker: Arc<AtomicWaker>,
}

impl Writer {
    pub fn push_slice(&mut self, cx: &mut std::task::Context<'_>, elems: &[u8]) -> Poll<usize> {
        let n = self.inner.push_slice(elems);
        if n == 0 {
            self.sendwaker.register(cx.waker());
            let n = self.inner.push_slice(elems);
            if n == 0 {
                return Poll::Pending;
            }
        }
        
        self.wake();
        Ready(n)
    }

    pub fn wake(&self) {
        self.recvwaker.wake();
    }
}

/// heap_size need != 0 && < usize::Max / 2
pub fn transfer(heap_size: usize) -> (Writer, Reader) {
    //0x3FFF * 20
    let rb = ringbuf::HeapRb::new(heap_size);
    let (w, r) = rb.split();
    let recvwaker = Arc::new(AtomicWaker::new());
    let sendwaker = Arc::new(AtomicWaker::new());
    (Writer { inner: w, recvwaker: recvwaker.clone(), sendwaker: sendwaker.clone() }, Reader { inner: r, recvwaker, sendwaker })
}