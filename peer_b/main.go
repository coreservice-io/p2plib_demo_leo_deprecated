package main

import (
	"fmt"
	"net"
	"time"

	"github.com/coreservice-io/p2plib_demo_leo/msg"
	"github.com/coreservice-io/p2plib_demo_leo/peer"
)

func main() {

	// msg_handlers
	peer.GetPeerManager().Reg_msg_handler(msg.CMD_TEST_INFO, func(param []byte) []byte {
		fmt.Println("CMD_TEST_INFO calldata:", string(param))
		return []byte("my info private-key of eth: xxx-xx-xx-xxxx-xxx-xxxx")
	})

	a_conn, err := net.Dial("tcp", "127.0.0.1:9090")
	if err != nil {
		fmt.Println(err)
		return
	}

	peer_con_a := &peer.Peer_conn{
		Conn: a_conn,
	}

	go peer_con_a.Run()

	//test some request
	chat, chat_err := peer_con_a.Request(msg.CMD_TEST_CHAT, []byte("jack"))
	fmt.Println(string(chat), chat_err)

	time.Sleep(30 * time.Second)

}
