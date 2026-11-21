//go:build linux

package filestore

import (
	"fmt"

	"golang.org/x/sys/unix"
)

// networkFS lists filesystems whose locking, append or fsync behavior cannot
// be trusted across machines (F11). FUSE is included because many network
// filesystems (sshfs, s3fs, gcsfuse) are built on it and its guarantees
// depend on the implementation.
var networkFS = map[uint32]string{
	unix.NFS_SUPER_MAGIC:  "NFS",
	unix.SMB_SUPER_MAGIC:  "SMB",
	unix.CIFS_SUPER_MAGIC: "CIFS",
	unix.SMB2_SUPER_MAGIC: "SMB2",
	unix.V9FS_MAGIC:       "9P",
	unix.FUSE_SUPER_MAGIC: "FUSE",
	unix.CEPH_SUPER_MAGIC: "Ceph",
	unix.AFS_SUPER_MAGIC:  "AFS",
	unix.AFS_FS_MAGIC:     "kAFS",
	unix.CODA_SUPER_MAGIC: "Coda",
	0x0bd00bd0:            "Lustre",
	0x47504653:            "GPFS",
}

// checkLocalFS refuses network filesystems (F11).
func checkLocalFS(dir string) error {
	var st unix.Statfs_t
	if err := unix.Statfs(dir, &st); err != nil {
		return fmt.Errorf("statfs %s: %w", dir, err)
	}
	// Statfs_t.Type is int32 on some architectures and int64 on others; the
	// magic numbers are 32-bit, so compare the low 32 bits.
	if name, ok := networkFS[uint32(st.Type)]; ok {
		return fmt.Errorf("%w: %s is on %s", ErrNetworkFS, dir, name)
	}
	return nil
}
