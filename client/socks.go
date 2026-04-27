package client

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"net"
	"time"
)

const (
	socks5Version    = 0x05
	noAuth           = 0x00
	noAcceptable     = 0xFF
	cmdConnect       = 0x01
	addrTypeIPv4     = 0x01
	addrTypeDomain   = 0x03
	addrTypeIPv6     = 0x04
	replySuccess     = 0x00
	replyFailure     = 0x01
	replyNotAllowed  = 0x02
	replyNetUnreach  = 0x03
	replyHostUnreach = 0x04
)

// SOCKSServer is a SOCKS5 proxy server that tunnels connections through GitHub.
type SOCKSServer struct {
	listen     string
	manager    *MuxClient
	timeout    time.Duration
	bufferSize int
}

// NewSOCKSServer creates a SOCKSServer. bufferSize is the per-direction
// io.Copy buffer used while relaying — sets the upper bound on bytes per
// clientConn.Read (which becomes one chunk in the next outbound frame).
// Pass <=0 to use io.Copy's default (32 KiB).
func NewSOCKSServer(listen string, manager *MuxClient, timeout time.Duration, bufferSize int) *SOCKSServer {
	return &SOCKSServer{listen: listen, manager: manager, timeout: timeout, bufferSize: bufferSize}
}

// ListenAndServe starts the SOCKS5 server. Blocks until ctx is cancelled.
func (s *SOCKSServer) ListenAndServe(ctx context.Context) error {
	ln, err := net.Listen("tcp", s.listen)
	if err != nil {
		return fmt.Errorf("socks5 listen on %s: %w", s.listen, err)
	}
	defer ln.Close()
	slog.Info("SOCKS5 proxy listening", "addr", s.listen)

	go func() {
		<-ctx.Done()
		ln.Close()
	}()

	for {
		conn, err := ln.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				return nil
			default:
				slog.Warn("socks5 accept error", "error", err)
				continue
			}
		}
		go s.handleConn(ctx, conn)
	}
}

func (s *SOCKSServer) handleConn(ctx context.Context, clientConn net.Conn) {
	defer clientConn.Close()
	_ = clientConn.SetDeadline(time.Now().Add(s.timeout))

	dst, err := s.handshake(clientConn)
	if err != nil {
		slog.Warn("socks5 handshake failed", "remote", clientConn.RemoteAddr(), "error", err)
		return
	}

	_ = clientConn.SetDeadline(time.Time{}) // clear deadline for relay phase

	vc, err := s.manager.Connect(ctx, dst)
	if err != nil {
		slog.Error("tunnel connect failed", "dst", dst, "error", err)
		writeSocksReply(clientConn, replyHostUnreach)
		return
	}
	defer s.manager.CloseConn(ctx, vc)

	writeSocksReply(clientConn, replySuccess)

	slog.Info("relaying", "dst", dst, "conn_id", vc.connID)

	done := make(chan struct{}, 2)
	go func() {
		if s.bufferSize > 0 {
			_, _ = io.CopyBuffer(vc, clientConn, make([]byte, s.bufferSize))
		} else {
			_, _ = io.Copy(vc, clientConn)
		}
		done <- struct{}{}
	}()
	go func() {
		if s.bufferSize > 0 {
			_, _ = io.CopyBuffer(clientConn, vc, make([]byte, s.bufferSize))
		} else {
			_, _ = io.Copy(clientConn, vc)
		}
		done <- struct{}{}
	}()

	select {
	case <-done:
	case <-ctx.Done():
	}
}

// handshake performs the SOCKS5 negotiation and returns the target address.
func (s *SOCKSServer) handshake(conn net.Conn) (string, error) {
	// Read greeting: VER NMETHODS METHODS...
	header := make([]byte, 2)
	if _, err := io.ReadFull(conn, header); err != nil {
		return "", fmt.Errorf("reading greeting header: %w", err)
	}
	if header[0] != socks5Version {
		return "", fmt.Errorf("unsupported SOCKS version %d", header[0])
	}
	nMethods := int(header[1])
	methods := make([]byte, nMethods)
	if _, err := io.ReadFull(conn, methods); err != nil {
		return "", fmt.Errorf("reading auth methods: %w", err)
	}

	// Select no-auth method
	supported := false
	for _, m := range methods {
		if m == noAuth {
			supported = true
			break
		}
	}
	if !supported {
		conn.Write([]byte{socks5Version, noAcceptable})
		return "", fmt.Errorf("no supported auth method (client offered %v)", methods)
	}
	conn.Write([]byte{socks5Version, noAuth})

	// Read request: VER CMD RSV ATYP DST.ADDR DST.PORT
	reqHeader := make([]byte, 4)
	if _, err := io.ReadFull(conn, reqHeader); err != nil {
		return "", fmt.Errorf("reading request header: %w", err)
	}
	if reqHeader[0] != socks5Version {
		return "", fmt.Errorf("bad SOCKS version in request: %d", reqHeader[0])
	}
	if reqHeader[1] != cmdConnect {
		writeSocksReply(conn, replyNotAllowed)
		return "", fmt.Errorf("unsupported command %d (only CONNECT supported)", reqHeader[1])
	}

	var host string
	switch reqHeader[3] {
	case addrTypeIPv4:
		addr := make([]byte, 4)
		if _, err := io.ReadFull(conn, addr); err != nil {
			return "", fmt.Errorf("reading IPv4: %w", err)
		}
		host = net.IP(addr).String()
	case addrTypeDomain:
		lenBuf := make([]byte, 1)
		if _, err := io.ReadFull(conn, lenBuf); err != nil {
			return "", fmt.Errorf("reading domain length: %w", err)
		}
		domain := make([]byte, lenBuf[0])
		if _, err := io.ReadFull(conn, domain); err != nil {
			return "", fmt.Errorf("reading domain: %w", err)
		}
		host = string(domain)
	case addrTypeIPv6:
		addr := make([]byte, 16)
		if _, err := io.ReadFull(conn, addr); err != nil {
			return "", fmt.Errorf("reading IPv6: %w", err)
		}
		host = "[" + net.IP(addr).String() + "]"
	default:
		writeSocksReply(conn, replyFailure)
		return "", fmt.Errorf("unsupported address type %d", reqHeader[3])
	}

	portBuf := make([]byte, 2)
	if _, err := io.ReadFull(conn, portBuf); err != nil {
		return "", fmt.Errorf("reading port: %w", err)
	}
	port := binary.BigEndian.Uint16(portBuf)

	return fmt.Sprintf("%s:%d", host, port), nil
}

// writeSocksReply sends a SOCKS5 reply with the given status code.
func writeSocksReply(conn net.Conn, status byte) {
	// Reply: VER REP RSV ATYP BND.ADDR BND.PORT
	reply := []byte{socks5Version, status, 0x00, addrTypeIPv4, 0, 0, 0, 0, 0, 0}
	_, _ = conn.Write(reply)
}
