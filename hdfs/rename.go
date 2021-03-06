package hdfs

import (
	hdfs "dmp_web/go/commons/hdfs/protocol/hadoop_hdfs"
	"dmp_web/go/commons/hdfs/rpc"
	"os"

	"github.com/golang/protobuf/proto"
)

// Rename renames (moves) a file.
func (c *Client) Rename(oldpath, newpath string) error {
	_, err := c.getFileInfo(newpath)
	if err == nil {
		return &os.PathError{"rename", newpath, os.ErrExist}
	} else if !os.IsNotExist(err) {
		return &os.PathError{"rename", newpath, err}
	}

	req := &hdfs.Rename2RequestProto{
		Src:           proto.String(oldpath),
		Dst:           proto.String(newpath),
		OverwriteDest: proto.Bool(true),
	}
	resp := &hdfs.Rename2ResponseProto{}

	err = c.namenode.Execute("rename2", req, resp)
	if err != nil {
		if nnErr, ok := err.(*rpc.NamenodeError); ok {
			err = interpretException(nnErr.Exception, err)
		}

		return &os.PathError{"rename", oldpath, err}
	}

	return nil
}
