# Netstack smoltcp by Kasesiki

[英文](https://github.com/Kasesiki/netstack-smoltcp/README.md)|中文

一个用于将IP协议流量转换为Tcp/Udp stream的专用网络栈，基于smoltcp

正在开发中

没有运行时依赖，高性能，易用

## 已支持
- Tcp传输

## 样例
查看 examples/minimal.rs

## 特性

- 没有运行时依赖，只依赖features和smoltcp crate,使用bytes与ringbuf进行数据传输

- 实现了features trait与tokio trait(可选的)

- 整个网络栈被包装在一个future中，没有额外线程，由用户驱动

## 为什么我要制作这个网络栈
当我用cavivie的netstack-smoltcp时，我遇到了无法解决的内存泄漏问题，同时对于个性化，具体的代码性能设计上，我也有所不满，他对每个tcpstream都带有锁设计，且高度绑定tokio，还需要运行额外的运行时，所以我设计了这个框架
没有运行时依赖，通过spsc无锁队列实现数据传输，简单易懂，没有内存泄漏，同时我希望为其打造更好的样例与文档，同时我保证他是纯手写的（无AI）

## 感谢
感谢这些非常不错的项目，是他们启发了netstack-smoltcp
- [cavivie's netstack-smoltcp](https://github.com/cavivie/netstack-smoltcp)
- [smoltcp](https://github.com/smoltcp-rs/smoltcp)