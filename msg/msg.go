package msg

import (
	"encoding/binary"
)

//////////////////////
// 0 - 999 reserved
const CMD_0 = 0
const CMD_ERR = 1 // communication error !important this is not used for application level error

////////////////////////
// 1000 - 1999 for p2p internal cmd

////////////////////////
// > 2000 for application cmd
const CMD_TEST_INFO = 4
const CMD_TEST_CHAT = 5

////////////////////////
const MSG_HEADER_SIZE = 17
const MSG_PAYLOAD_SIZE_LIMIT = 1024 * 16 //16KB
const MSG_CHUNKS_LIMIT = 1024            //16MB size limit for the maxium size of a single message

type MsgHeader struct {
	Version      uint32 //2.x => version 2 , 3.x => version 3 , different versions not compatable
	Id           uint32 //a unique random number which assigned by request side
	Cmd          uint32
	EOF          uint8 //1 for end of message
	Payload_size uint32
}

func Encode_msg_header(msg_h *MsgHeader) []byte {
	header_bytes := make([]byte, 17)

	binary.LittleEndian.PutUint32(header_bytes[0:4], msg_h.Version)
	binary.LittleEndian.PutUint32(header_bytes[4:8], msg_h.Id)
	binary.LittleEndian.PutUint32(header_bytes[8:12], msg_h.Cmd)
	header_bytes[12] = byte(msg_h.EOF)
	binary.LittleEndian.PutUint32(header_bytes[13:17], msg_h.Payload_size)

	return header_bytes
}

func Decode_msg_header(msg_header []byte) *MsgHeader {

	return &MsgHeader{
		Version:      binary.LittleEndian.Uint32(msg_header[0:4]),
		Id:           binary.LittleEndian.Uint32(msg_header[4:8]),
		Cmd:          binary.LittleEndian.Uint32(msg_header[8:12]),
		EOF:          uint8(msg_header[12]),
		Payload_size: binary.LittleEndian.Uint32(msg_header[13:17]),
	}
}

func Encode_msg(msg_h *MsgHeader, payload []byte) []byte {
	return append(Encode_msg_header(msg_h), payload...)
}
