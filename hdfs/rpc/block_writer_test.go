package rpc

import (
	hdfs "dmp_web/go/commons/hdfs/protocol/hadoop_hdfs"
	"hash/crc32"
	"io"
	"os"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func createBlock(t *testing.T, name string) *BlockWriter {
	namenode := getNamenode(t)
	blockSize := int64(1048576)

	createReq := &hdfs.CreateRequestProto{
		Src:          proto.String(name),
		Masked:       &hdfs.FsPermissionProto{Perm: proto.Uint32(uint32(0644))},
		ClientName:   proto.String(ClientName),
		CreateFlag:   proto.Uint32(1),
		CreateParent: proto.Bool(false),
		Replication:  proto.Uint32(uint32(3)),
		BlockSize:    proto.Uint64(uint64(blockSize)),
	}
	createResp := &hdfs.CreateResponseProto{}

	err := namenode.Execute("create", createReq, createResp)
	require.NoError(t, err)

	addBlockReq := &hdfs.AddBlockRequestProto{
		Src:        proto.String(name),
		ClientName: proto.String(ClientName),
		Previous:   nil,
	}
	addBlockResp := &hdfs.AddBlockResponseProto{}

	err = namenode.Execute("addBlock", addBlockReq, addBlockResp)
	require.NoError(t, err)

	block := addBlockResp.GetBlock()
	return NewBlockWriter(block, namenode, blockSize)
}

func finishBlock(t *testing.T, name string, bw *BlockWriter) {
	namenode := getNamenode(t)

	err := bw.Close()
	require.NoError(t, err)

	completeReq := &hdfs.CompleteRequestProto{
		Src:        proto.String(name),
		ClientName: proto.String(ClientName),
		Last:       bw.block.GetB(),
	}
	completeResp := &hdfs.CompleteResponseProto{}

	err = namenode.Execute("complete", completeReq, completeResp)
	require.NoError(t, err)
}

func baleet(t *testing.T, name string) {
	namenode := getNamenode(t)

	req := &hdfs.DeleteRequestProto{
		Src:       proto.String(name),
		Recursive: proto.Bool(true),
	}
	resp := &hdfs.DeleteResponseProto{}

	err := namenode.Execute("delete", req, resp)
	require.NoError(t, err)
	require.NotNil(t, resp.Result)
}

func TestWriteFailsOver(t *testing.T) {
	name := "/_test/create/6.txt"
	baleet(t, name)

	mobydick, err := os.Open("../test/mobydick.txt")
	require.NoError(t, err)

	bw := createBlock(t, name)
	bw.connectNext()
	bw.stream.ackError = ackError{0, 0, hdfs.Status_ERROR}

	_, err = io.CopyN(bw, mobydick, 1048576)
	require.NoError(t, err)
	finishBlock(t, name, bw)

	br, _ := getBlockReader(t, name)
	hash := crc32.NewIEEE()
	n, err := io.Copy(hash, br)
	require.NoError(t, err)
	assert.EqualValues(t, 1048576, n)
	assert.EqualValues(t, 0xb35a6a0e, hash.Sum32())
}
