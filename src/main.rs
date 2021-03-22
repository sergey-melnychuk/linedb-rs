use tokio::fs::File;
use tokio::io::{self, AsyncReadExt, AsyncWriteExt, AsyncSeekExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::{self, Sender};
use tokio_util::codec::{LinesCodec, Framed};
use futures::{StreamExt, SinkExt};
use bytes::{BytesMut, BufMut, Buf};
use log::{info, debug, error};
use std::io::SeekFrom;
use std::time::Instant;
use std::path::Path;


#[tokio::main]
async fn main() -> io::Result<()> {
    env_logger::init();
    info!("started");

    let mut now = Instant::now();
    let dat = "uuid.dat";
    let idx = "uuid.idx";
    let n = 10000000;
    dump(dat, n).await?;
    info!("dumping data file completed in {} ms", now.elapsed().as_millis());
    index(dat, idx).await?;
    now = Instant::now();
    info!("indexing data file completed in {} ms", now.elapsed().as_millis());

    let (tx, mut rx) = mpsc::channel(1);
    let listener = TcpListener::bind("127.0.0.1:10322").await?;

    loop {
        let tx = tx.clone();

        tokio::select! {
            _ = rx.recv() => {
                info!("shutdown command received");
                break;
            }
            accepted = listener.accept() => {
                match accepted {
                    Ok((socket, address)) => {
                        debug!("got connection from {}", address);
                        tokio::spawn(async move {
                            let _ = handle(socket, idx, dat, tx).await;
                            debug!("connection closed from {}", address);
                        });
                    },
                    Err(e) => debug!("error accepting connection; error = {:?}", e)
                }
            }
        }
    }

    info!("server stopped");
    Ok(())
}

async fn handle(socket: TcpStream, idx: &str, dat: &str, tx: Sender<()>) -> io::Result<()> {
    let mut idx = File::open(idx).await?;
    let mut dat = File::open(dat).await?;

    let mut lines = Framed::new(socket, LinesCodec::new());

    while let Some(result) = lines.next().await {
        match result {
            Ok(line) => {
                info!("received line: '{}'", line);

                if line == "QUIT" {
                    let _ = lines.send("BYE").await;
                    return Ok(());
                }

                if line == "SHUTDOWN" {
                    let _ = tx.send(()).await;
                    let _ = lines.send("STOPPING SERVER").await;
                    return Ok(());
                }

                let mut socket = lines.into_inner();
                lookup(&mut idx, &mut dat, &line, &mut socket).await?;
                lines = Framed::new(socket, LinesCodec::new());
                if let Err(e) = lines.send("").await {
                    error!("failed to send response EOL: error = {:?}", e);
                }
            },
            Err(e) => {
                error!("error decoding string from socket; error = {:?}", e);
            }
        }
    }

    Ok(())
}

async fn lookup(idx: &mut File, dat: &mut File, line: &str, socket: &mut TcpStream) -> io::Result<()> {
    const PAGE_SIZE: usize = 4 * 1024;
    if !line.starts_with("GET ") {
        socket.write_all(b"ERR: invalid command").await?;
        socket.flush().await?;
        return Ok(())
    }

    let mut it = line.split(' ');
    let _ = it.next(); // skip 'GET '
    let number = it.next().unwrap_or_default();

    let n: u64 = {
        if let Ok(parsed) = number.parse::<u64>() {
            parsed
        } else {
            socket.write_all(b"ERR: invalid line number").await?;
            socket.flush().await?;
            return Ok(());
        }
    };
    debug!("parsed line number to lookup: {}", n);

    if n == 0 {
        socket.write_all(b"ERR: no such line").await?;
        socket.flush().await?;
        return Ok(());
    }

    let offset: u64 = (n - 1) * std::mem::size_of::<u64>() as u64;
    if offset > dat.metadata().await?.len() as u64 {
        socket.write_all(b"ERR: no such line").await?;
        socket.flush().await?;
        return Ok(());
    }

    idx.seek(SeekFrom::Start(offset)).await?;
    let that = idx.read_u64().await? + (if offset > 0 {1} else {0});
    let next = idx.read_u64().await?;
    let len = (next - that) as usize;
    debug!("index lookup: offset={} length={}", that, len);

    // TODO investigate how to do zero-copy response
    // TODO investigate how to avoid manual copying
    dat.seek(SeekFrom::Start(that)).await?;
    socket.write_all(b"OK\n").await?;
    if len > PAGE_SIZE {
        let mut buf = BytesMut::with_capacity(PAGE_SIZE);
        let mut remaining = len;
        while remaining > 0 {
            let n = dat.read_buf(&mut buf).await?;
            if n == 0 {
                break;
            }

            socket.write(&buf[..n]).await?;
            debug!("response chunk written, togo={} len={}", remaining, len);
            buf.clear();

            remaining -= n;
        }
    } else {
        let mut buf = BytesMut::with_capacity(len);
        let n = dat.read_buf(&mut buf).await?;
        socket.write(&buf[..n]).await?;
    }
    socket.flush().await?;
    debug!("response completed: {} bytes", len);

    Ok(())
}

async fn index(dat: &str, idx: &str) -> io::Result<()> {
    const PAGE_SIZE: usize = 4 * 1024;
    if Path::new(idx).exists() {
        return Ok(());
    }

    let mut dat = File::open(dat).await?;
    let mut idx = File::create(idx).await?;

    let mut rbuf = BytesMut::with_capacity(PAGE_SIZE);
    let mut wbuf = BytesMut::with_capacity(PAGE_SIZE);

    let mut offset: u64 = 0;
    wbuf.put_u64(0);
    loop {
        let n = dat.read_buf(&mut rbuf).await?;
        if n == 0 {
            break;
        }

        for i in 0..n {
            if rbuf[i] == b'\n' {
                let entry: u64 = offset + (i as u64);

                if wbuf.capacity() - wbuf.remaining() < std::mem::size_of::<u64>() {
                    idx.write_buf(&mut wbuf).await?;
                    wbuf.clear();
                }
                wbuf.put_u64(entry);
            }
        }
        rbuf.clear();
        offset += n as u64;
    }

    wbuf.put_u64(offset);
    idx.write_buf(&mut wbuf).await?;
    idx.flush().await?;
    Ok(())
}

async fn dump(dat: &str, n: usize) -> io::Result<()> {
    const PAGE_SIZE: usize = 4 * 1024;
    if Path::new(dat).exists() {
        return Ok(());
    }

    let mut dat = File::create(dat).await?;
    let mut buf = BytesMut::with_capacity(PAGE_SIZE);

    for _ in 0..n {
        let id = uuid::Uuid::new_v4().to_string();
        if buf.capacity() - buf.remaining() < id.len() + 1 {
            dat.write_buf(&mut buf).await?;
            buf.clear();
        }
        buf.put_slice(id.as_bytes());
        buf.put_u8(b'\n');
    }

    dat.write_buf(&mut buf).await?;
    dat.flush().await?;
    Ok(())
}
