package hdfs

import (
	hdfs "dmp_web/go/commons/hdfs/protocol/hadoop_hdfs"
	"dmp_web/go/commons/hdfs/rpc"
	"os"
	"path"

	"github.com/golang/protobuf/proto"
)

// Mkdir creates a new directory with the specified name and permission bits.
func (c *Client) Mkdir(dirname string, perm os.FileMode) error {
	return c.mkdir(dirname, perm, false)
}

// MkdirAll creates a directory for dirname, along with any necessary parents,
// and returns nil, or else returns an error. The permission bits perm are used
// for all directories that MkdirAll creates. If dirname is already a directory,
// MkdirAll does nothing and returns nil.
func (c *Client) MkdirAll(dirname string, perm os.FileMode) error {
	return c.mkdir(dirname, perm, true)
}

func (c *Client) mkdir(dirname string, perm os.FileMode, createParent bool) error {
	dirname = path.Clean(dirname)

	_, err := c.getFileInfo(dirname)
	if err == nil {
		return &os.PathError{"mkdir", dirname, os.ErrExist}
	} else if !os.IsNotExist(err) {
		return &os.PathError{"mkdir", dirname, err}
	}

	req := &hdfs.MkdirsRequestProto{
		Src:          proto.String(dirname),
		Masked:       &hdfs.FsPermissionProto{Perm: proto.Uint32(uint32(perm))},
		CreateParent: proto.Bool(createParent),
	}
	resp := &hdfs.MkdirsResponseProto{}

	err = c.namenode.Execute("mkdirs", req, resp)
	if err != nil {
		if nnErr, ok := err.(*rpc.NamenodeError); ok {
			err = interpretException(nnErr.Exception, err)
		}

		return &os.PathError{"mkdir", dirname, err}
	}

	return nil
}
