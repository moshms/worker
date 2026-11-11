use std::io::{Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::time::Duration;

use anyhow::{bail, Context};

const DEFAULT_URL: &str = "http://127.0.0.1:8081/readyz";

pub fn run() -> anyhow::Result<()> {
    let url = std::env::var("MC_HEALTHCHECK_URL").unwrap_or_else(|_| DEFAULT_URL.to_owned());
    let rest = url
        .strip_prefix("http://")
        .with_context(|| format!("MC_HEALTHCHECK_URL must use http://, got {url}"))?;

    let (authority, path) = match rest.split_once('/') {
        Some((authority, path)) => (authority, format!("/{path}")),
        None => (rest, "/".to_owned()),
    };

    let address = authority
        .to_socket_addrs()
        .with_context(|| format!("resolve {authority}"))?
        .next()
        .with_context(|| format!("resolve {authority}"))?;

    let mut stream = TcpStream::connect_timeout(&address, Duration::from_secs(2))
        .with_context(|| format!("connect to {address}"))?;
    stream.set_read_timeout(Some(Duration::from_secs(2)))?;
    stream.set_write_timeout(Some(Duration::from_secs(2)))?;

    write!(
        stream,
        "GET {path} HTTP/1.1\r\nHost: {authority}\r\nConnection: close\r\n\r\n"
    )?;

    let mut buffer = [0_u8; 256];
    let read = stream.read(&mut buffer)?;
    let response = String::from_utf8_lossy(&buffer[..read]);
    let status = response.lines().next().unwrap_or_default();

    if status.contains(" 200 ") {
        Ok(())
    } else {
        bail!("unhealthy response: {status}");
    }
}
