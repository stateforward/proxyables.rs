
use futures::io::{AsyncRead, AsyncWrite, self};
use std::pin::Pin;
use std::task::{Context, Poll, Waker};
use std::sync::{Arc, Mutex};
use std::collections::VecDeque;
use futures::channel::mpsc;
use super::frame::Frame;
use super::consts::*;

#[derive(Debug)]
pub(crate) struct StreamState {
    pub id: u32,
    pub read_buf: VecDeque<u8>,
    pub read_waker: Option<Waker>,
    pub write_sender: mpsc::UnboundedSender<Frame>, // Using unbounded for simplicity, could be bounded
    pub closed: bool,
    pub remote_closed: bool,
}

#[derive(Debug, Clone)]
pub struct StreamHandle {
    state: Arc<Mutex<StreamState>>,
}

impl StreamHandle {
    pub(crate) fn new(id: u32, write_sender: mpsc::UnboundedSender<Frame>) -> Self {
        Self {
            state: Arc::new(Mutex::new(StreamState {
                id,
                read_buf: VecDeque::new(),
                read_waker: None,
                write_sender,
                closed: false,
                remote_closed: false,
            })),
        }
    }

    pub(crate) fn feed_data(&self, data: Vec<u8>) {
        let mut state = self.state.lock().unwrap();
        if state.remote_closed { return; }
        state.read_buf.extend(data);
        if let Some(waker) = state.read_waker.take() {
            waker.wake();
        }
    }

    pub(crate) fn on_fin(&self) {
        let mut state = self.state.lock().unwrap();
        state.remote_closed = true;
        if let Some(waker) = state.read_waker.take() {
            waker.wake();
        }
    }
}

impl AsyncRead for StreamHandle {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        let mut state = self.state.lock().unwrap();
        
        if !state.read_buf.is_empty() {
            let len = std::cmp::min(buf.len(), state.read_buf.len());
            for i in 0..len {
                buf[i] = state.read_buf.pop_front().unwrap();
            }
            return Poll::Ready(Ok(len));
        }

        if state.remote_closed {
            return Poll::Ready(Ok(0)); // EOF
        }

        state.read_waker = Some(cx.waker().clone());
        Poll::Pending
    }
}

impl AsyncWrite for StreamHandle {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let state = self.state.lock().unwrap();
        if state.closed {
            return Poll::Ready(Err(io::Error::new(io::ErrorKind::BrokenPipe, "Stream closed")));
        }
        
        // Construct frame
        let frame = Frame::new(TYPE_DATA, 0, state.id, buf.to_vec());
        match state.write_sender.unbounded_send(frame) {
            Ok(_) => Poll::Ready(Ok(buf.len())),
            Err(_) => Poll::Ready(Err(io::Error::new(io::ErrorKind::BrokenPipe, "Session closed"))),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let mut state = self.state.lock().unwrap();
        if state.closed { return Poll::Ready(Ok(())); }
        state.closed = true;
        
        let frame = Frame::new(TYPE_DATA, FLAG_FIN, state.id, vec![]);
        let _ = state.write_sender.unbounded_send(frame);
        Poll::Ready(Ok(()))
    }
}
