package peer

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
const PEER_MSG_WRITE_TIMEOUT_SECS = 8 //peer ->1 msg -> peer  write max duration in seconds
const PEER_MSG_READ_TIMEOUT_SECS = 8  //peer <- 1 msg <- peer  read max duration in seconds

//////////////////////////////////////////////////////////////////

var peer_manager *PeerManager = &PeerManager{}

func GetPeerManager() *PeerManager {
	return peer_manager
}

type PeerManager struct {
	Msg_handlers sync.Map //msg_cmd => func(*Msg_channel)
}

func (pm *PeerManager) Reg_msg_handler(msg_cmd uint32, handler func([]byte) []byte) {
	pm.Msg_handlers.Store(msg_cmd, handler)
}

//return nil if not exist
func (pm *PeerManager) Get_msg_handler(msg_cmd uint32) func([]byte) []byte {
	if h_i, ok := pm.Msg_handlers.Load(msg_cmd); ok {
		return h_i.(func([]byte) []byte)
	} else {
		return nil
	}
}

//////////////////////////////////////////////////////////////////

type Peer_msg struct {
	Id         uint32
	Msg_cmd    uint32
	Msg_chunks chan int32 //incoming message chunk size [chunk1_size,chunk2_size....] , -1 for eof of message
	Msg_buffer *bytes.Buffer
	Msg_err    bool
}

func (peer_msg *Peer_msg) Receive() ([]byte, error) {

	total_chunks := 0
	eof := false

	for {

		select {

		case msg_chunk_size := <-peer_msg.Msg_chunks:

			if msg_chunk_size == -1 {
				eof = true
			} else {
				total_chunks++
			}

		case <-time.After(time.Duration(PEER_MSG_READ_TIMEOUT_SECS) * time.Second):
			return nil, errors.New("PEER_MSG_READ_TIMEOUT_SECS")
		}

		if total_chunks > msg.MSG_CHUNKS_LIMIT {
			return nil, errors.New("msg chunk over limit,total chunks:" + strconv.Itoa(total_chunks))
		}

		if eof {
			if peer_msg.Msg_err {
				return nil, errors.New(peer_msg.Msg_buffer.String())
			}
			return peer_msg.Msg_buffer.Bytes(), nil
		}

	}

}

type Peer_conn struct {
	Conn     net.Conn
	Messages sync.Map
}

func (pc *Peer_conn) send_msg(msg_id uint32, msg_cmd uint32, raw_msg []byte) error {

	r_msg_len := len(raw_msg)

	//chunks >=1
	chunks := int(math.Ceil(float64(r_msg_len) / msg.MSG_PAYLOAD_SIZE_LIMIT))
	if chunks == 0 {
		chunks = 1
	}

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

		pc.Conn.SetWriteDeadline(time.Now().Add(PEER_MSG_WRITE_TIMEOUT_SECS * time.Second))
		_, w_err := pc.Conn.Write(msg.Encode_msg(&msg.MsgHeader{
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

func (pc *Peer_conn) Request(msg_cmd uint32, raw_msg []byte) ([]byte, error) {

	p_msg := &Peer_msg{
		Id:         rand.Uint32(),
		Msg_cmd:    msg_cmd,
		Msg_chunks: make(chan int32, msg.MSG_CHUNKS_LIMIT+1), //+1 for eof
		Msg_buffer: bytes.NewBuffer([]byte{}),
	}

	pc.Messages.Store(p_msg.Id, p_msg)
	defer pc.Messages.Delete(p_msg.Id)

	//encode the message and send chunk by chunk with writetime out
	err := pc.send_msg(p_msg.Id, msg_cmd, raw_msg)
	if err != nil {
		return nil, err
	}
	//after send finished , start to receive from msg_buffer chunk by chunk with readtimeout
	r, r_err := p_msg.Receive()
	if r_err != nil {
		return nil, r_err
	}

	return r, nil
}

func (peer_conn *Peer_conn) Run() {

	defer func() {
		peer_conn.Conn.Close()
		fmt.Println("peer conn exit")
	}()

	reader := bufio.NewReader(peer_conn.Conn)
	header_buf := make([]byte, msg.MSG_HEADER_SIZE)
	payload_buf := make([]byte, msg.MSG_PAYLOAD_SIZE_LIMIT)

	for {

		_, err := io.ReadFull(reader, header_buf[:])
		if err != nil {
			if err != io.EOF {
				fmt.Println("handle_request header conn io read err:", err)
			}
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
			_, err := io.ReadFull(reader, payload_buf[0:msg_header.Payload_size])
			if err != nil {
				fmt.Println("handle_request payload conn io read err:", err)
				return
			}
		}

		/////////////////////////////////////////
		if msg_header.Cmd == msg.CMD_ERR || msg_header.Cmd == msg.CMD_0 {

			if peer_msg_i, exist := peer_conn.Messages.Load(msg_header.Id); exist {

				if msg_header.Payload_size > 0 {
					peer_msg_i.(*Peer_msg).Msg_buffer.Write(payload_buf[0:msg_header.Payload_size])
				}
				peer_msg_i.(*Peer_msg).Msg_chunks <- int32(msg_header.Payload_size)

				if msg_header.EOF == 1 {
					peer_msg_i.(*Peer_msg).Msg_chunks <- -1
				}

				if msg_header.Cmd == msg.CMD_ERR {
					peer_msg_i.(*Peer_msg).Msg_err = true
				}

			} else {
				fmt.Println("handle_request msg_id not found, id:", msg_header.Id)
			}

		} else {
			if _, exist := peer_conn.Messages.Load(msg_header.Id); exist {
				fmt.Println("handle_request msg_id overlap, id:", msg_header.Id)
				return //must return not continue as consequent message will bring chaos if continue
			}

			if GetPeerManager().Get_msg_handler(msg_header.Cmd) == nil {
				fmt.Println("msg_handler not found, msg_cmd:", msg_header.Cmd)
				return //must return not continue as consequent message will bring chaos if continue
			}

			p_msg := &Peer_msg{
				Id:         msg_header.Id,
				Msg_cmd:    msg_header.Cmd,
				Msg_chunks: make(chan int32, msg.MSG_CHUNKS_LIMIT+1), //+1 for eof
				Msg_buffer: bytes.NewBuffer([]byte{}),
			}

			if msg_header.Payload_size > 0 {
				p_msg.Msg_buffer.Write(payload_buf[0:msg_header.Payload_size])
			}

			p_msg.Msg_chunks <- int32(msg_header.Payload_size)

			if msg_header.EOF == 1 {
				p_msg.Msg_chunks <- -1
			}

			peer_conn.Messages.Store(msg_header.Id, p_msg)

			//start receive process
			go func(peer_msg *Peer_msg, nc *Peer_conn) {

				defer peer_conn.Messages.Delete(peer_msg.Id)

				calldata, err := peer_msg.Receive()

				if err != nil {
					nc.send_msg(peer_msg.Id, msg.CMD_ERR, []byte(err.Error()))
					fmt.Println(err)
					return
				}

				//send call result
				result := GetPeerManager().Get_msg_handler(msg_header.Cmd)(calldata)
				nc.send_msg(peer_msg.Id, msg.CMD_0, result)

			}(p_msg, peer_conn)

		}

	}
}

func init() {
	rand.Seed(time.Now().UTC().UnixNano())
}
