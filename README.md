# Netstack smoltcp by Kasesiki

英文|[中文](https://github.com/Kasesiki/netstack-smoltcp/README_CN.md)

A netstack for the special purpose of turning packets from/to a TUN interface into TCP streams and UDP packets. It uses smoltcp-rs as the backend netstack.

In development.

no runtime depend, high perfermance, easy useful.

## Supported
- Tcp transport

## Example
see examples/minimal.rs

## Features

- no runtime depend, just use features and smoltcp crate, use ringbuf and bytes(zero copy)

- Implements popular future streaming traitsand asynchronous IO traits for futures and tokio(optional).

- user driven data updates and timer. just a future, no new thread.

## Why make the netstack
When I used cavivie's netstack-smoltcp, I encountered memory leak issues that I could not resolve. At the same time, I was also dissatisfied with the customization and specific code performance design. It has a lock design for every tcpstream, is highly bound to tokio, and also requires running an additional runtime, so I designed this framework.

It has no runtime dependency and implements data transmission through an spsc lock-free queue. It is simple and easy to understand, with no memory leaks. At the same time, I hope to create better examples and documentation for it, and I guarantee that it is purely handwritten (no AI).

This description was written by me in Chinese and then translated into English by AI.

## Inspired By

Special thanks to these amazing projects that inspired netstack-smoltcp (in no particular order):
- [cavivie's netstack-smoltcp](https://github.com/cavivie/netstack-smoltcp)
- [smoltcp](https://github.com/smoltcp-rs/smoltcp)