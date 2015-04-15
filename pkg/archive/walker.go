package archive

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"syscall"
	"time"
	"unsafe"

	"github.com/docker/docker/pkg/system"
)

type walker struct {
	dir1  string
	dir2  string
	root1 *FileInfo
	root2 *FileInfo
	q     chan workItem
}

// collectFileInfoForChanges returns a complete representation of the trees
// rooted at dir1 and dir2, with one important exception: any subtree or
// leaf where the inode and device numbers are an exact match between dir1
// and dir2 will be pruned from the results. This method is *only* to be used
// to generating a list of changes between the two directories, as it does not
// reflect the full contents.
func collectFileInfoForChanges(dir1, dir2 string) (*FileInfo, *FileInfo, error) {
	w := &walker{
		dir1:  dir1,
		dir2:  dir2,
		root1: newRootFileInfo(),
		root2: newRootFileInfo(),
		q:     make(chan workItem, 1024*1024),
	}

	i1, err := os.Lstat(w.dir1)
	if err != nil {
		return nil, nil, err
	}
	i2, err := os.Lstat(w.dir2)
	if err != nil {
		return nil, nil, err
	}

	w.q <- workItem{"/", i1, i2}

	conc := 4
	ch := make(chan error, conc)
	for i := 0; i < conc; i++ {
		go func() { ch <- w.walkQueue() }()
		time.Sleep(3 * time.Millisecond)
	}

	for i := 0; i < conc; i++ {
		if err := <-ch; err != nil {
			return nil, nil, err
		}
	}

	return w.root1, w.root2, nil
}

func walkchunk(path string, fi os.FileInfo, dir string, root *FileInfo) error {
	if fi == nil {
		return nil
	}
	parent := root.LookUp(filepath.Dir(path))
	if parent == nil {
		return fmt.Errorf("collectFileInfoForChanges: Unexpectedly no parent for %s", path)
	}
	info := &FileInfo{
		name:     filepath.Base(path),
		children: make(map[string]*FileInfo),
		parent:   parent,
	}
	cpath := filepath.Join(dir, path)
	stat, err := system.Lstat(cpath) //////////// !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
	if err != nil {
		return err
	}
	info.stat = stat
	info.capability, _ = system.Lgetxattr(cpath, "security.capability") //////////// !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
	parent.AddChild(info)
	parent.children[info.name] = info
	return nil
}

type workItem struct {
	path string
	i1   os.FileInfo
	i2   os.FileInfo
}

func (w *walker) walkQueue() (err error) {
	for {
		select {
		case item := <-w.q:
			if err := w.work(item.path, item.i1, item.i2); err != nil {
				return err
			}
		default:
			return nil
		}
	}
}

func (w *walker) work(path string, i1, i2 os.FileInfo) (err error) {
	if path != "/" {
		if err := walkchunk(path, i1, w.dir1, w.root1); err != nil {
			return err
		}
		if err := walkchunk(path, i2, w.dir2, w.root2); err != nil {
			return err
		}
	}

	is1Dir := i1 != nil && i1.IsDir()
	is2Dir := i2 != nil && i2.IsDir()

	// If these files are both non-existent, or leaves (non-dirs), we are done.
	if !is1Dir && !is2Dir {
		return nil
	}

	var names1, names2 []nameIno
	if is1Dir {
		names1, err = readDirNames(filepath.Join(w.dir1, path)) //////////// !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
		if err != nil {
			return err
		}
	}

	if is2Dir {
		names2, err = readDirNames(filepath.Join(w.dir2, path)) //////////// !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
		if err != nil {
			return err
		}
	}

	var names []string
	ix1 := 0
	ix2 := 0

	for {
		if ix1 >= len(names1) {
			break
		}
		if ix2 >= len(names2) {
			break
		}

		ni1 := names1[ix1]
		ni2 := names2[ix2]

		switch bytes.Compare([]byte(ni1.name), []byte(ni2.name)) {
		case -1: // ni1 < ni2 -- advance ni1
			// we will not encounter ni1 in names2
			names = append(names, ni1.name)
			ix1++
		case 0: // ni1 == ni2
			if ni1.ino != ni2.ino {
				names = append(names, ni1.name)
			}
			ix1++
			ix2++
		case 1: // ni1 > ni2 -- advance ni2
			// we will not encounter ni2 in names1
			names = append(names, ni2.name)
			ix2++
		}
	}
	for ix1 < len(names1) {
		names = append(names, names1[ix1].name)
		ix1++
	}
	for ix2 < len(names2) {
		names = append(names, names2[ix2].name)
		ix2++
	}

	for _, name := range names {
		fname := filepath.Join(path, name)
		fp1 := filepath.Join(w.dir1, fname)
		fp2 := filepath.Join(w.dir2, fname)
		cInfo1, err1 := os.Lstat(fp1) //////////// !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
		cInfo2, err2 := os.Lstat(fp2) //////////// !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
		if err1 != nil && !os.IsNotExist(err1) {
			return err
		}
		if err2 != nil && !os.IsNotExist(err2) {
			return err
		}
		w.q <- workItem{fname, cInfo1, cInfo2}
	}
	return nil
}

func readDirNames(dirname string) ([]nameIno, error) {
	f, err := os.Open(dirname)
	if err != nil {
		return nil, err
	}
	names, err := readdirnames(f)
	f.Close()
	if err != nil {
		return nil, err
	}
	sl := nameInoSlice(names)
	sort.Sort(sl)
	return sl, nil
}

const (
	blockSize = 4096
)

type dirInfo struct {
	buf  []byte // buffer for directory I/O
	nbuf int    // length of buf; return value from Getdirentries
	bufp int    // location of next record in buf.
}

type nameIno struct {
	name string
	ino  uint64
}

type nameInoSlice []nameIno

func (s nameInoSlice) Len() int           { return len(s) }
func (s nameInoSlice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s nameInoSlice) Less(i, j int) bool { return s[i].name < s[j].name }

func readdirnames(f *os.File) (names []nameIno, err error) {
	size := 100
	n := -1

	d := new(dirInfo)
	d.buf = make([]byte, 4096)

	names = make([]nameIno, 0, size) // Empty with room to grow.
	for n != 0 {
		// Refill the buffer if necessary
		if d.bufp >= d.nbuf {
			d.bufp = 0
			var errno error
			d.nbuf, errno = fixCount(syscall.ReadDirent(int(f.Fd()), d.buf)) // getdents on linux
			if errno != nil {
				return names, os.NewSyscallError("readdirent", errno)
			}
			if d.nbuf <= 0 {
				break // EOF
			}
		}

		// Drain the buffer
		var nb, nc int
		nb, nc, names = ParseDirent(d.buf[d.bufp:d.nbuf], n, names)
		d.bufp += nb
		n -= nc
	}
	if n >= 0 && len(names) == 0 {
		return names, io.EOF
	}
	return names, nil
}

func fixCount(n int, err error) (int, error) {
	if n < 0 {
		n = 0
	}
	return n, err
}

func ParseDirent(buf []byte, max int, names []nameIno) (consumed int, count int, newnames []nameIno) {
	origlen := len(buf)
	count = 0
	for max != 0 && len(buf) > 0 {
		dirent := (*syscall.Dirent)(unsafe.Pointer(&buf[0]))
		buf = buf[dirent.Reclen:]
		if dirent.Ino == 0 { // File absent in directory.
			continue
		}
		bytes := (*[10000]byte)(unsafe.Pointer(&dirent.Name[0]))
		var name = string(bytes[0:clen(bytes[:])])
		if name == "." || name == ".." { // Useless names
			continue
		}
		max--
		count++
		names = append(names, nameIno{name, dirent.Ino})
	}
	return origlen - len(buf), count, names
}

func clen(n []byte) int {
	for i := 0; i < len(n); i++ {
		if n[i] == 0 {
			return i
		}
	}
	return len(n)
}
