package escapeRoomItemClient

import (
	"fmt"
	"github.com/jam23541/escapeRoomDataTypes"
	"github.com/jam23541/go_plugins"
	"github.com/jam23541/idDataChain"
	"time"
)

// each item client corresponds to a particular item id in the game
type ItemClient struct {
	ReSendInterval                       time.Duration // time it waits until resend, in milliseconds
	ItemId                               string
	EffectState                          map[string]string
	GameState                            string
	PoolOfUniqueCodes                    idDataChain.IdDataChain
	ChanMqttMsgToPub                     chan escapeRoomDataTypes.MqttMsg // stores all the msg to pub to iot devices; all msg to be published will be put in here
	ChanMsgReceive                       chan escapeRoomDataTypes.MqttMsg // stores all msg received
	MsgPubing                            escapeRoomDataTypes.MqttMsg
	MsgPubingTimeBuffer                  time.Time
	MsgSendToSystemMqttPubChannelTimeOut time.Duration // time it waits to give up when trying to put msg into system mqtt pub channel, in millis
	ChanSystemMqttPub                    *chan escapeRoomDataTypes.MqttMsg
	ChanSystemNatsPub                    *chan escapeRoomDataTypes.MqttMsg
}

// PoolOfUniqueCodesHandler takes in a new code and
// 1. check if it is a duplicated msg, return -1 if so
// 2. if not, add msg to the pool and make sure the pool has length capped at max, return 1
func (myClient *ItemClient) poolOfUniqueCodesHandler(newCode string) int {
	return myClient.PoolOfUniqueCodes.Put(newCode)
}

func (myClient *ItemClient) SendRoutine() {
	for {
		if len(myClient.ChanMqttMsgToPub) > 0 {
			// check if stucked
			if (myClient.MsgPubing.UNIQUEID == "") || (myClient.MsgPubing.UNIQUEID == "EMPTY") {
				myClient.MsgPubing = <-myClient.ChanMqttMsgToPub
				myClient.MsgPubing.UNIQUEID = go_plugins.UniqueId(myClient.MsgPubing.PUBER)
				timeout := time.NewTimer(time.Millisecond * (myClient.MsgSendToSystemMqttPubChannelTimeOut))
				select {
				case *myClient.ChanSystemMqttPub <- myClient.MsgPubing:
					myClient.MsgPubingTimeBuffer = time.Now()
				case <-timeout.C:
					fmt.Println("blocked")
				}
			} else { // it is stucked , resend the msg
				if time.Now().Sub(myClient.MsgPubingTimeBuffer) >= myClient.ReSendInterval {
					timeout := time.NewTimer(time.Millisecond * (myClient.MsgSendToSystemMqttPubChannelTimeOut))
					select {
					case *myClient.ChanSystemMqttPub <- myClient.MsgPubing:
						myClient.MsgPubingTimeBuffer = time.Now()
					case <-timeout.C:
						fmt.Println("blocked")
					}
				}
			}
		}
	}
}

// 1. deal with reply msg, i.e. unblock the send routine
// 2. deal with duplicated msg, put new msg into system nats pub channel
func (myClient *ItemClient) ReceiveRoutine() {
	for {
		// mqtt msg received, check if its reply msg
		select {
		case newMsg := <-myClient.ChanMsgReceive:

			switch newMsg.MSGTYPE {
			case string(ReplyMsg):
				//check if it is the msg this client is waiting for
				uniqueCodeReplyFor := newMsg.DATA[0]
				if (uniqueCodeReplyFor == myClient.MsgPubing.UNIQUEID) && (myClient.MsgPubing.UNIQUEID != "") {
					myClient.MsgPubing.UNIQUEID = ""
				}
			default:
				// check if it is a duplicated msg
				uniqueCode := newMsg.UNIQUEID
				switch myClient.poolOfUniqueCodesHandler(uniqueCode) {
				case 1: // new msg
					*myClient.ChanSystemNatsPub <- newMsg
					break
				case -1: // duplicated msg
					break
				}
			}
		}
	}

}

func (myClient *ItemClient) Reset() {
	myClient.MsgPubing = escapeRoomDataTypes.MqttMsg{}
	myClient.PoolOfUniqueCodes.Reset()
	myClient.EffectState = make(map[string]string)
	myClient.GameState = ""

}

func NewItemClient(itemId string, natsChannel *chan escapeRoomDataTypes.MqttMsg, mqttChannel *chan escapeRoomDataTypes.MqttMsg) *ItemClient {

	return &ItemClient{
		ReSendInterval:                       1000,
		ItemId:                               itemId,
		EffectState:                          make(map[string]string),
		GameState:                            "",
		PoolOfUniqueCodes:                    *idDataChain.NewIdDataChain(),
		ChanMqttMsgToPub:                     make(chan escapeRoomDataTypes.MqttMsg, 100),
		ChanMsgReceive:                       make(chan escapeRoomDataTypes.MqttMsg, 100),
		MsgPubing:                            escapeRoomDataTypes.MqttMsg{},
		MsgPubingTimeBuffer:                  time.Time{},
		MsgSendToSystemMqttPubChannelTimeOut: 500,
		ChanSystemMqttPub:                    mqttChannel,
		ChanSystemNatsPub:                    natsChannel,
	}
}
