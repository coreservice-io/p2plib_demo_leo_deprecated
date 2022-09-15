package main

import (
	"fmt"
	"net"

	"github.com/coreservice-io/p2plib_demo_leo/msg"
	"github.com/coreservice-io/p2plib_demo_leo/node"
)

func main() {

	// msg_handlers
	node.GetNodeManager().Reg_msg_handler(msg.CMD_TEST_CHAT, func(param []byte) []byte {
		fmt.Println("CMD_TEST_CHAT calldata:", string(param))
		return []byte("hello:" + string(param))
	})

	var node_con_b *node.Node_conn

	////////////////////////////////////////////

	l, err := net.Listen("tcp", ":9090")
	if err != nil {
		panic(err)
	}
	defer l.Close()

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting: ", err.Error())
			continue
		}

		node_con_b = &node.Node_conn{Conn: conn}
		go node_con_b.Run()

		go func() {
			//do some test request to node_b
			info, info_err := node_con_b.Request(msg.CMD_TEST_INFO, []byte("want to know some of your info"))
			fmt.Println(string(info), info_err)

		}()
	}

}
