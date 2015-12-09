package zerorpc

import (
	"github.com/samuel/go-zookeeper/zk"
	"path"
)

const (
	PERM_FILE      = zk.PermAdmin | zk.PermRead | zk.PermWrite
	PERM_DIRECTORY = zk.PermAdmin | zk.PermCreate | zk.PermDelete | zk.PermRead | zk.PermWrite
)

func DefaultACLs() []zk.ACL {
	return zk.WorldACL(zk.PermAll)
}

func DefaultDirACLs() []zk.ACL {
	return zk.WorldACL(PERM_DIRECTORY)
}

func DefaultFileACLs() []zk.ACL {
	return zk.WorldACL(PERM_FILE)
}

func ErrorEqual(a, b error) bool {
	if a != nil && b != nil {
		return a.Error() == b.Error()
	}
	return a == b
}

func CreateRecursive(conn *zk.Conn, zkPath, value string, flags int, acls []zk.ACL) (createdPath string, err error) {
	createdPath, err = conn.Create(zkPath, []byte(value), int32(flags), acls)
	if ErrorEqual(err, zk.ErrNoNode) {
		dirAcls := make([]zk.ACL, len(acls))
		for i, acl := range acls {
			dirAcls[i] = acl
			dirAcls[i].Perms = PERM_DIRECTORY
		}
		_, err = CreateRecursive(conn, path.Dir(zkPath), "", flags, dirAcls)
		if err != nil && !ErrorEqual(err, zk.ErrNodeExists) {
			return "", err
		}
		createdPath, err = conn.Create(zkPath, []byte(value), int32(flags), acls)
	} else if ErrorEqual(err, zk.ErrNodeExists) {
		return zkPath, nil
	}
	return
}

func WatchChildren(conn *zk.Conn, path string, onChange func(children []string)) {
	for {
		children, _, ch, err := conn.ChildrenW(path)
		if err != nil {
			//log.Printf("watch children path error, path:%s, err:%v\n", path, err)
			continue
		}
		onChange(children)
		select {
		case <-ch:
		}
	}
}
