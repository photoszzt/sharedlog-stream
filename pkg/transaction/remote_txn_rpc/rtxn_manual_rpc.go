package remote_txn_rpc

import (
	"bytes"
	context "context"
	"fmt"
	"net/http"
	commtypes "sharedlog-stream/pkg/commtypes"
	txn_data "sharedlog-stream/pkg/txn_data"
	"time"

	"github.com/tinylib/msgp/msgp"
)

type RemoteTxnClient interface {
	Init(ctx context.Context, in *InitArg) (*InitReply, error)
	AppendTpPar(ctx context.Context, in *txn_data.TxnMetaMsg) error
	AbortTxn(ctx context.Context, in *txn_data.TxnMetaMsg) error
	CommitTxnAsyncComplete(ctx context.Context, in *txn_data.TxnMetaMsg) (*CommitReply, error)
	AppendConsumedOffset(ctx context.Context, in *ConsumedOffsets) error
}

var RTxnFuncName = "remoteTxnMngr"

type RTxnRpcClient struct {
	client         *http.Client
	faas_gateway   string
	funcUrl        string
	nodeConstraint string
	readBuffer     []byte
	serde          commtypes.SerdeG[*RTxnArg]
}

func NewRTxnRpcClient(faas_gateway string, nodeConstraint string) RTxnRpcClient {
	return RTxnRpcClient{
		client: &http.Client{
			Transport: &http.Transport{
				IdleConnTimeout: 30 * time.Second,
			},
			Timeout: time.Duration(5) * time.Second,
		},
		faas_gateway:   faas_gateway,
		funcUrl:        fmt.Sprintf("http://%s/function/%s", faas_gateway, RTxnFuncName),
		nodeConstraint: nodeConstraint,
		readBuffer:     make([]byte, 0, 1024),
		serde:          GetRTxnArgSerdeG(),
	}
}

func postReqOnly(client *http.Client, url, nodeConstraint string, request []byte) error {
	req, err := http.NewRequest("POST", url, bytes.NewReader(request))
	if err != nil {
		return err
	}
	if nodeConstraint != "" {
		req.Header.Add("X-Faas-Node-Constraint", nodeConstraint)
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return fmt.Errorf("Non-OK response: %d", resp.StatusCode)
	}
	return nil
}

func postRequest[V msgp.Decodable](client *http.Client, url, nodeConstraint string, request []byte, response V) error {
	req, err := http.NewRequest("POST", url, bytes.NewReader(request))
	if err != nil {
		return err
	}
	if nodeConstraint != "" {
		req.Header.Add("X-Faas-Node-Constraint", nodeConstraint)
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return fmt.Errorf("Non-OK response: %d", resp.StatusCode)
	}
	reader := msgp.NewReader(resp.Body)
	response.DecodeMsg(reader)
	return nil
}

func (c *RTxnRpcClient) Init(ctx context.Context, in *InitArg) (*InitReply, error) {
	arg := &RTxnArg{
		RpcType: Init,
		Init:    in,
	}
	ret, err := c.serde.Encode(arg)
	if err != nil {
		return nil, err
	}
	reply := InitReply{}
	err = postRequest(c.client, c.funcUrl, c.nodeConstraint, ret, &reply)
	if err != nil {
		return nil, err
	}
	return &reply, nil
}

func (c *RTxnRpcClient) AppendTpPar(ctx context.Context, in *txn_data.TxnMetaMsg) error {
	arg := &RTxnArg{
		RpcType: AppendTpPar,
		MetaMsg: in,
	}
	ret, err := c.serde.Encode(arg)
	if err != nil {
		return err
	}
	return postReqOnly(c.client, c.funcUrl, c.nodeConstraint, ret)
}

func (c *RTxnRpcClient) AbortTxn(ctx context.Context, in *txn_data.TxnMetaMsg) error {
	arg := &RTxnArg{
		RpcType: AbortTxn,
		MetaMsg: in,
	}
	ret, err := c.serde.Encode(arg)
	if err != nil {
		return err
	}
	return postReqOnly(c.client, c.funcUrl, c.nodeConstraint, ret)
}

func (c *RTxnRpcClient) CommitTxnAsyncComplete(ctx context.Context, in *txn_data.TxnMetaMsg) (*CommitReply, error) {
	arg := &RTxnArg{
		RpcType: CommitTxnAsync,
		MetaMsg: in,
	}
	ret, err := c.serde.Encode(arg)
	if err != nil {
		return nil, err
	}
	reply := CommitReply{}
	err = postRequest(c.client, c.funcUrl, c.nodeConstraint, ret, &reply)
	if err != nil {
		return nil, err
	}
	return &reply, nil
}

func (c *RTxnRpcClient) AppendConsumedOffset(ctx context.Context, in *ConsumedOffsets) error {
	arg := &RTxnArg{
		RpcType:     AppendConsumedOff,
		ConsumedOff: in,
	}
	ret, err := c.serde.Encode(arg)
	if err != nil {
		return err
	}
	return postReqOnly(c.client, c.funcUrl, c.nodeConstraint, ret)
}
