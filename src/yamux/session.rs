
use futures::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use futures::channel::mpsc;
use futures::{select, FutureExt, StreamExt};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::io;

use super::frame::Frame;
use super::stream::StreamHandle;
use super::consts::*;

struct SessionState {
    next_stream_id: u32,
    streams: HashMap<u32, StreamHandle>,
}

#[derive(Clone)]
pub struct Session {
    state: Arc<Mutex<SessionState>>,
    write_sender: mpsc::UnboundedSender<Frame>,
}

pub struct SessionDriver<T> {
    stream: T,
    state: Arc<Mutex<SessionState>>,
    write_receiver: mpsc::UnboundedReceiver<Frame>,
    accept_sender: mpsc::UnboundedSender<StreamHandle>,
    write_sender: mpsc::UnboundedSender<Frame>,
}

impl Session {
    pub fn new<T>(stream: T, is_client: bool) -> (Self, SessionDriver<T>, mpsc::UnboundedReceiver<StreamHandle>)
    where T: AsyncRead + AsyncWrite + Unpin {
        let (write_sd, write_rc) = mpsc::unbounded();
        let (accept_sd, accept_rc) = mpsc::unbounded();
        
        let state = Arc::new(Mutex::new(SessionState {
            next_stream_id: if is_client { 1 } else { 2 },
            streams: HashMap::new(),
        }));
        
        let session = Session {
            state: state.clone(),
            write_sender: write_sd.clone(),
        };
        
        let driver = SessionDriver {
            stream,
            state,
            write_receiver: write_rc,
            accept_sender: accept_sd,
            write_sender: write_sd,
        };
        
        (session, driver, accept_rc)
    }

    pub fn open_stream(&self) -> io::Result<StreamHandle> {
        let mut state = self.state.lock().unwrap();
        let id = state.next_stream_id;
        state.next_stream_id += 2;
        
        let stream = StreamHandle::new(id, self.write_sender.clone());
        state.streams.insert(id, stream.clone());
        
        let frame = Frame::control(TYPE_WINDOW_UPDATE, FLAG_SYN, id, WINDOW_SIZE);
        self.write_sender.unbounded_send(frame).map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "Failed to send SYN"))?;
        
        Ok(stream)
    }
}

impl<T: AsyncRead + AsyncWrite + Unpin> SessionDriver<T> {
    pub async fn run(self) -> io::Result<()> {
        let (mut reader, mut writer) = self.stream.split();
        let state = self.state;
        let mut write_receiver = self.write_receiver;
        let accept_sender = self.accept_sender;
        let write_sender = self.write_sender;
        
        // Helper to handle frame logic
        let handle_frame_fn = |frame: Frame| -> io::Result<()> {
             let stream_id = frame.header.stream_id;
             let flags = frame.header.flags;
             let typ = frame.header.typ;

             if typ == TYPE_PING {
                 if flags & FLAG_SYN != 0 {
                     let _ = write_sender.unbounded_send(Frame::control(TYPE_PING, FLAG_ACK, 0, frame.header.length));
                 }
                 return Ok(());
             }

             if typ == TYPE_GO_AWAY {
                 return Ok(());
             }

             let stream_opt = {
                 let mut s = state.lock().unwrap();
                 if flags & FLAG_SYN != 0 {
                     if !s.streams.contains_key(&stream_id) {
                          let stream = StreamHandle::new(stream_id, write_sender.clone());
                          s.streams.insert(stream_id, stream.clone());
                          let _ = accept_sender.unbounded_send(stream);
                          let _ = write_sender.unbounded_send(Frame::control(TYPE_WINDOW_UPDATE, FLAG_ACK, stream_id, WINDOW_SIZE));
                     }
                 }
                 s.streams.get(&stream_id).cloned()
             };
             
             if let Some(stream) = stream_opt {
                  if typ == TYPE_DATA {
                      if !frame.payload.is_empty() {
                          stream.feed_data(frame.payload);
                      }
                  }
                  if flags & FLAG_FIN != 0 {
                      stream.on_fin();
                  }
                  if flags & FLAG_RST != 0 {
                      stream.on_fin();
                      let mut s = state.lock().unwrap();
                      s.streams.remove(&stream_id);
                  }
             }
             Ok(())
        };

        let mut header_buf = [0u8; HEADER_LENGTH];
        
        let mut read_future = reader.read_exact(&mut header_buf).fuse();
        let mut write_future = write_receiver.next().fuse();
        
        let mut reading_body = false;
        let mut current_header: Option<super::frame::FrameHeader> = None;
        let mut body_buf: Vec<u8> = vec![];

        loop {
            select! {
                res = read_future => {
                    match res {
                        Ok(_) => {
                            if !reading_body {
                                match super::frame::Frame::decode_header(&header_buf) {
                                    Ok(header) => {
                                        current_header = Some(header.clone());
                                        if header.typ == TYPE_DATA && header.length > 0 {
                                            reading_body = true;
                                            body_buf = vec![0u8; header.length as usize];
                                            read_future = reader.read_exact(&mut body_buf).fuse();
                                        } else {
                                            let frame = Frame { header: header.clone(), payload: vec![] };
                                            handle_frame_fn(frame)?;
                                            read_future = reader.read_exact(&mut header_buf).fuse();
                                        }
                                    },
                                    Err(_) => return Err(io::Error::new(io::ErrorKind::InvalidData, "Invalid Frame")),
                                }
                            } else {
                                if let Some(header) = current_header.take() {
                                    let frame = Frame { header, payload: std::mem::take(&mut body_buf) };
                                    handle_frame_fn(frame)?;
                                }
                                reading_body = false;
                                read_future = reader.read_exact(&mut header_buf).fuse();
                            }
                        },
                        Err(_) => return Err(io::Error::new(io::ErrorKind::ConnectionAborted, "Read Error")),
                    }
                },
                frame = write_future => {
                    if let Some(frame) = frame {
                        let data = frame.encode();
                        writer.write_all(&data).await?;
                        write_future = write_receiver.next().fuse();
                    } else {
                        return Ok(());
                    }
                }
            }
        }
    }
}
