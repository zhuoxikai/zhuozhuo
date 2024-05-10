package main

import (
	"fmt"
	"io"
	"log"
	"net"
)

// Conn 是你需要实现的一种连接类型，它支持下面描述的若干接口；
// 为了实现这些接口，你需要设计一个基于 TCP 的简单协议；
type Conn struct {
	n *net.TCPConn
}

type ConnWriter struct {
	conn *Conn
}

const FIN = "FIN"

func (c *ConnWriter) Write(p []byte) (n int, err error) {
	if n, err = c.conn.n.Write(p); err != nil {
		log.Println("write data error:", err)
		return
	}
	return
}
func (c *ConnWriter) Close() error {
	if _, err := c.conn.n.Write([]byte(FIN)); err != nil {
		return err
	}
	return nil
}

type ConnReader struct {
	conn *Conn
}

func (c *ConnReader) Read(p []byte) (n int, err error) {
	if n, err = c.conn.n.Read(p); string(p[:n]) == FIN {
		log.Println("read io EOF")
		err = io.EOF
		return 0, err
	} else if err != nil {
		log.Println("read data error:", err)
		return
	}
	return
}

// Send 传入一个 key 表示发送者将要传输的数据对应的标识；
// 返回 writer 可供发送者分多次写入大量该 key 对应的数据；
// 当发送者已将该 key 对应的所有数据写入后，调用 writer.Close 告知接收者：该 key 的数据已经完全写入；
func (conn *Conn) Send(key string) (writer io.WriteCloser, err error) {
	//send key to receiver
	if _, err = conn.n.Write([]byte(key)); err != nil {
		log.Println("send key to receiver error:", err)
		return
	}
	log.Println("send key success key:", key)
	//make writer
	w := &ConnWriter{
		conn: conn,
	}

	return w, nil
}

// Receive 返回一个 key 表示接收者将要接收到的数据对应的标识；
// 返回的 reader 可供接收者多次读取该 key 对应的数据；
// 当 reader 返回 io.EOF 错误时，表示接收者已经完整接收该 key 对应的数据；
func (conn *Conn) Receive() (key string, reader io.Reader, err error) {
	var n int
	buf := make([]byte, 1024)
	//read key
	if n, err = conn.n.Read(buf); err != nil {
		log.Println("receiver read key error:", err)
		return
	}
	key = string(buf[:n])
	log.Println("read key success key:", key)
	//make reader
	r := &ConnReader{
		conn: conn,
	}

	return key, r, nil
}

// Close 关闭你实现的连接对象及其底层的 TCP 连接
func (conn *Conn) Close() {
	conn.n.Close()
}

// NewConn 从一个 TCP 连接得到一个你实现的连接对象
func NewConn(conn net.Conn) *Conn {
	tcpConn := conn.(*net.TCPConn)
	newConn := &Conn{
		n: tcpConn,
	}
	return newConn
}

// 除了上面规定的接口，你还可以自行定义新的类型，变量和函数以满足实现需求

//////////////////////////////////////////////
///////// 接下来的代码为测试代码，请勿修改 /////////
//////////////////////////////////////////////

// 连接到测试服务器，获得一个你实现的连接对象
func dial(serverAddr string) *Conn {
	conn, err := net.Dial("tcp", serverAddr)
	if err != nil {
		panic(err)
	}
	return NewConn(conn)
}

// 启动测试服务器
func startServer(handle func(*Conn)) net.Listener {
	ln, err := net.Listen("tcp", ":5566")
	if err != nil {
		panic(err)
	}
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				fmt.Println("[WARNING] ln.Accept", err)
				return
			}
			go handle(NewConn(conn))
		}
	}()
	return ln
}

// 简单断言
func assertEqual(actual string, expected string) {
	if actual != expected {
		panic(fmt.Sprintf("actual:%v expected:%v\n", actual, expected))
	}
	log.Println("assert success!")
}

// 简单 case：单连接，双向传输少量数据
func testCase0() {
	const (
		key  = "Bible"
		data = `Then I heard the voice of the Lord saying, “Whom shall I send? And who will go for us?”
And I said, “Here am I. Send me!”
Isaiah 6:8`
	)
	ln := startServer(func(conn *Conn) {
		// 服务端等待客户端进行传输
		_key, reader, err := conn.Receive()
		if err != nil {
			panic(err)
		}
		assertEqual(_key, key)
		dataB, err := io.ReadAll(reader)
		if err != nil {
			panic(err)
		}
		assertEqual(string(dataB), data)
		// 服务端向客户端进行传输
		writer, err := conn.Send(key)
		if err != nil {
			panic(err)
		}
		n, err := writer.Write([]byte(data))
		if err != nil {
			panic(err)
		}
		if n != len(data) {
			panic(n)
		}
		conn.Close()
	})
	//goland:noinspection GoUnhandledErrorResult
	defer ln.Close()

	conn := dial(ln.Addr().String())
	// 客户端向服务端传输
	writer, err := conn.Send(key)
	if err != nil {
		panic(err)
	}
	n, err := writer.Write([]byte(data))
	if n != len(data) {
		panic(n)
	}
	err = writer.Close()
	if err != nil {
		panic(err)
	}
	// 客户端等待服务端传输
	_key, reader, err := conn.Receive()
	if err != nil {
		panic(err)
	}
	assertEqual(_key, key)
	dataB, err := io.ReadAll(reader)
	if err != nil {
		panic(err)
	}
	assertEqual(string(dataB), data)
	conn.Close()
}
