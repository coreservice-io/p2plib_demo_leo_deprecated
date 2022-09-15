package node

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/coreservice-io/p2plib_demo_leo/msg"
)

const VERSION = 1
const NODE_MSG_WRITE_TIMEOUT_SECS = 8 //node ->1 msg -> node  write max duration in seconds
const NODE_MSG_READ_TIMEOUT_SECS = 8  //node <- 1 msg <- node  read max duration in seconds

//////////////////////////////////////////////////////////////////

var node_manager *NodeManager = &NodeManager{}

func GetNodeManager() *NodeManager {
	return node_manager
}

type NodeManager struct {
	Msg_handlers sync.Map //msg_cmd => func(*Msg_channel)
}

func (nm *NodeManager) Reg_msg_handler(msg_cmd uint32, handler func([]byte) []byte) {
	nm.Msg_handlers.Store(msg_cmd, handler)
}

//return nil if not exist
func (nm *NodeManager) Get_msg_handler(msg_cmd uint32) func([]byte) []byte {
	if h_i, ok := nm.Msg_handlers.Load(msg_cmd); ok {
		return h_i.(func([]byte) []byte)
	} else {
		return nil
	}
}

//////////////////////////////////////////////////////////////////

type Node_msg struct {
	Id         uint32
	Msg_cmd    uint32
	Msg_chunks chan string
	Msg_err    bool
}

func (node_msg *Node_msg) Receive() ([]byte, error) {

	total_chunks := 0
	eof := false
	buffer := new(bytes.Buffer)

	for {

		select {
		case node_msg := <-node_msg.Msg_chunks:
			total_chunks++
			if node_msg != "" {
				node_msg_bytes := []byte(node_msg)
				_, err := buffer.Write(node_msg_bytes[1:])
				if err != nil {
					return nil, err
				}
				//end of message chunk
				if node_msg_bytes[0] == 1 {
					eof = true
				}
			}

		case <-time.After(time.Duration(NODE_MSG_READ_TIMEOUT_SECS) * time.Second):
			return nil, errors.New("NODE_MSG_READ_TIMEOUT_SECS")
		}

		if total_chunks > msg.MSG_CHUNKS_LIMIT {
			return nil, errors.New("msg chunk over limit,total chunks:" + strconv.Itoa(total_chunks))
		}

		if eof {
			if node_msg.Msg_err {
				return nil, errors.New(buffer.String())
			}
			return buffer.Bytes(), nil
		}

	}

}

type Node_conn struct {
	Conn     net.Conn
	Messages sync.Map
}

func (nc *Node_conn) send_msg(msg_id uint32, msg_cmd uint32, raw_msg []byte) error {

	r_msg_len := len(raw_msg)

	chunks := int(math.Ceil(float64(r_msg_len) / msg.MSG_PAYLOAD_SIZE_LIMIT)) //chunks >=1

	if chunks > msg.MSG_CHUNKS_LIMIT {
		return errors.New("send_msg chunks over limit, chunks:" + strconv.Itoa(chunks))
	}

	for i := 0; i < chunks; i++ {

		var msg_cmd_ uint32 = msg.CMD_0
		var msg_eof_ uint8 = 0

		if i == 0 {
			msg_cmd_ = msg_cmd
		}

		if i == chunks-1 {
			msg_eof_ = 1
		}

		start := i * msg.MSG_PAYLOAD_SIZE_LIMIT
		end := (i + 1) * msg.MSG_PAYLOAD_SIZE_LIMIT

		if end >= r_msg_len {
			end = r_msg_len
		}

		nc.Conn.SetWriteDeadline(time.Now().Add(NODE_MSG_WRITE_TIMEOUT_SECS * time.Second))
		_, w_err := nc.Conn.Write(msg.Encode_msg(&msg.MsgHeader{
			Version:      VERSION,
			Cmd:          msg_cmd_,
			EOF:          msg_eof_,
			Id:           msg_id,
			Payload_size: uint32(end - start),
		}, raw_msg[start:end]))

		if w_err != nil {
			return w_err
		}

	}

	return nil

}

func (nc *Node_conn) Request(msg_cmd uint32, raw_msg []byte) ([]byte, error) {

	n_msg := &Node_msg{
		Id:         rand.Uint32(),
		Msg_cmd:    msg_cmd,
		Msg_chunks: make(chan string, msg.MSG_CHUNKS_LIMIT),
	}

	nc.Messages.Store(n_msg.Id, n_msg)
	defer nc.Messages.Delete(n_msg.Id)

	//encode the message and send chunk by chunk with writetime out
	err := nc.send_msg(n_msg.Id, msg_cmd, raw_msg)
	if err != nil {
		return nil, err
	}
	//after send finished , start to receive from msg_buffer chunk by chunk with readtimeout
	r, r_err := n_msg.Receive()
	if r_err != nil {
		return nil, r_err
	}

	return r, nil
}

func (node_conn *Node_conn) Run() {

	defer func() {
		node_conn.Conn.Close()
		fmt.Println("node conn exit")
	}()

	reader := bufio.NewReader(node_conn.Conn)
	header_buf := make([]byte, msg.MSG_HEADER_SIZE)
	payload_buf := make([]byte, msg.MSG_PAYLOAD_SIZE_LIMIT)

	for {

		header_n, err := io.ReadFull(reader, header_buf[:])
		if err != nil {
			if err != io.EOF {
				fmt.Println("handle_request header conn io read err:", err)
			}
			return
		}

		if header_n != msg.MSG_HEADER_SIZE {
			fmt.Println("handle_request header_n != msg.MSG_HEADER_SIZE")
			return
		}

		msg_header := msg.Decode_msg_header(header_buf[:])

		//check version ,payload size ,...etc..
		if msg_header.Payload_size > msg.MSG_PAYLOAD_SIZE_LIMIT {
			fmt.Println("handle_request msg_header Payload_size oversize:", msg_header.Payload_size)
			return
		}

		//read payload
		if msg_header.Payload_size > 0 {
			payload_n, err := io.ReadFull(reader, payload_buf[0:msg_header.Payload_size])
			if err != nil {
				fmt.Println("handle_request payload conn io read err:", err)
				return
			}

			if payload_n != int(msg_header.Payload_size) {
				fmt.Println("handle_request payload_n != msg_header.Payload_size")
				return
			}
		}

		/////////////////////////////////////////
		if msg_header.Cmd == msg.CMD_ERR || msg_header.Cmd == msg.CMD_0 {

			if node_msg_i, exist := node_conn.Messages.Load(msg_header.Id); exist {

				node_msg_i.(*Node_msg).Msg_chunks <- string([]byte{msg_header.EOF}) + string(payload_buf[0:msg_header.Payload_size])

				// if msg_header.EOF == 1 {
				// 	node_msg_i.(*Node_msg).Msg_eof <- struct{}{}
				// }

				if msg_header.Cmd == msg.CMD_ERR {
					node_msg_i.(*Node_msg).Msg_err = true
				}

			} else {
				fmt.Println("handle_request msg_id not found, id:", msg_header.Id)
			}

		} else {
			if _, exist := node_conn.Messages.Load(msg_header.Id); exist {
				fmt.Println("handle_request msg_id overlap, id:", msg_header.Id)
				return //must return not continue as consequent message will bring chaos if continue
			}

			if GetNodeManager().Get_msg_handler(msg_header.Cmd) == nil {
				fmt.Println("msg_handler not found, msg_cmd:", msg_header.Cmd)
				return //must return not continue as consequent message will bring chaos if continue
			}

			n_msg := &Node_msg{
				Id:         msg_header.Id,
				Msg_cmd:    msg_header.Cmd,
				Msg_chunks: make(chan string, msg.MSG_CHUNKS_LIMIT+1), //+1 for EOF tag
			}

			n_msg.Msg_chunks <- string([]byte{msg_header.EOF}) + string(payload_buf[0:msg_header.Payload_size])

			//n_msg.Msg_chunks <- string(payload_buf[0:msg_header.Payload_size])
			// if msg_header.EOF == 1 {
			// 	n_msg.Msg_eof <- struct{}{}
			// }

			node_conn.Messages.Store(msg_header.Id, n_msg)

			//start receive process
			go func(node_msg *Node_msg, nc *Node_conn) {

				defer node_conn.Messages.Delete(node_msg.Id)

				calldata, err := node_msg.Receive()

				if err != nil {
					nc.send_msg(node_msg.Id, msg.CMD_ERR, []byte(err.Error()))
					fmt.Println(err)
					return
				}

				//send call result
				result := GetNodeManager().Get_msg_handler(msg_header.Cmd)(calldata)
				nc.send_msg(node_msg.Id, msg.CMD_0, result)

			}(n_msg, node_conn)

		}

	}
}

func init() {
	rand.Seed(time.Now().UTC().UnixNano())
}
