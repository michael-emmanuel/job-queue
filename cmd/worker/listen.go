package main

import "net"

func listen(addr string) (net.Listener, error) { return net.Listen("tcp", addr) }
