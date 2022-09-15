package main

import (
	"fmt"
	"net"

	"github.com/coreservice-io/p2plib_demo_leo/msg"
	"github.com/coreservice-io/p2plib_demo_leo/node"
)

func main() {

	// msg_handlers
	node.GetNodeManager().Reg_msg_handler(msg.CMD_TEST_INFO, func(param []byte) []byte {
		fmt.Println("CMD_TEST_INFO calldata:", string(param))
		return []byte("my info private-key of eth: xxx-xx-xx-xxxx-xxx-xxxx")
	})

	a_conn, err := net.Dial("tcp", "127.0.0.1:9090")
	if err != nil {
		fmt.Println(err)
		return
	}

	node_con_a := &node.Node_conn{
		Conn: a_conn,
	}

	go node_con_a.Run()

	//test some request
	chat, chat_err := node_con_a.Request(msg.CMD_TEST_CHAT, []byte("tommy"))
	fmt.Println(string(chat), chat_err)

}
