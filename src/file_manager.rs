use fuser::{FileAttr, FileType};
use id_tree::{InsertBehavior, Node, NodeId, Tree, TreeBuilder};
use pna::{Archive, DataKind, ReadOption};
use std::cell::OnceCell;
use std::collections::HashMap;
use std::ffi::OsString;
use std::io::Read;
use std::ops::Add;
use std::path::{Path, PathBuf};
use std::time::SystemTime;
use std::{fs, io};

pub type Inode = u64;

pub(crate) struct Entry {
    cell: OnceCell<Vec<u8>>,
    data: Option<(pna::RegularEntry, ReadOption)>,
}

impl Entry {
    fn empty() -> Self {
        Self {
            cell: Default::default(),
            data: None,
        }
    }

    pub(crate) fn as_slice(&mut self) -> &[u8] {
        self.cell
            .get_or_init(|| {
                if let Some((entry, option)) = self.data.take() {
                    let mut buf = Vec::new();
                    let mut reader = entry.reader(option).unwrap();
                    reader.read_to_end(&mut buf).unwrap();
                    buf
                } else {
                    Vec::new()
                }
            })
            .as_slice()
    }
}

pub(crate) struct File {
    pub(crate) name: OsString,
    pub(crate) data: Entry,
    pub(crate) attr: FileAttr,
}

impl File {
    fn dir(inode: Inode, name: OsString) -> Self {
        let now = SystemTime::now();
        Self {
            name,
            data: Entry::empty(),
            attr: FileAttr {
                ino: inode,
                size: 512,
                blocks: 1,
                atime: now,
                mtime: now,
                ctime: now,
                crtime: now,
                kind: FileType::Directory,
                perm: 0o775,
                nlink: 2,
                uid: 0,
                gid: 0,
                rdev: 0,
                blksize: 0,
                flags: 0,
            },
        }
    }
    fn root(inode: Inode) -> Self {
        Self::dir(inode, ".".into())
    }

    fn from_entry<S: Into<String>>(
        inode: Inode,
        entry: pna::RegularEntry,
        password: Option<S>,
    ) -> Self {
        let now = SystemTime::now();
        let header = entry.header();
        let metadata = entry.metadata();
        let option = ReadOption::with_password(password);
        let raw_size = {
            let mut size = 0;
            let mut reader = entry.reader(option.clone()).unwrap();
            let mut buf = [0u8; 1024];
            while let Ok(s) = reader.read(&mut buf) {
                if s == 0 {
                    break;
                }
                size += s;
            }
            size
        };
        Self {
            name: header
                .path()
                .as_path()
                .components()
                .last()
                .unwrap()
                .as_os_str()
                .into(),
            attr: FileAttr {
                ino: inode,
                size: raw_size as u64,
                blocks: 1,
                atime: now,
                mtime: metadata
                    .modified()
                    .map(|it| SystemTime::UNIX_EPOCH.add(it))
                    .unwrap_or(now),
                ctime: metadata
                    .modified()
                    .map(|it| SystemTime::UNIX_EPOCH.add(it))
                    .unwrap_or(now),
                crtime: metadata
                    .created()
                    .map(|it| SystemTime::UNIX_EPOCH.add(it))
                    .unwrap_or(now),
                kind: match header.data_kind() {
                    DataKind::File => FileType::RegularFile,
                    DataKind::Directory => FileType::Directory,
                    DataKind::SymbolicLink => FileType::Symlink,
                    DataKind::HardLink => FileType::RegularFile,
                },
                perm: 0o775,
                nlink: 1,
                uid: 0,
                gid: 0,
                rdev: 0,
                blksize: 512,
                flags: 0,
            },
            data: Entry {
                cell: Default::default(),
                data: Some((entry, option)),
            },
        }
    }
}

const ROOT_INODE: Inode = 1;

pub(crate) struct FileManager {
    archive_path: PathBuf,
    password: Option<String>,
    tree: Tree<Inode>,
    files: HashMap<Inode, File>,
    node_ids: HashMap<Inode, NodeId>,
    last_inode: Inode,
}

impl FileManager {
    pub(crate) fn new(archive_path: PathBuf) -> Self {
        let mut mamager = Self {
            archive_path,
            password: None,
            tree: TreeBuilder::new().build(),
            files: HashMap::new(),
            node_ids: HashMap::new(),
            last_inode: ROOT_INODE,
        };
        mamager.populate().unwrap();
        mamager
    }

    fn populate(&mut self) -> io::Result<()> {
        self.add_root_file(File::root(ROOT_INODE))?;
        let file = fs::File::open(&self.archive_path)?;
        let mut archive = Archive::read_header(file)?;
        let password = self.password.clone();
        for entry in archive.entries_with_password(password.as_deref()) {
            let entry = entry?;
            let mut parents = entry.header().path().as_path().parent();
            let parent = if let Some(parents) = parents {
                self.make_dir_all(parents, ROOT_INODE)?
            } else {
                ROOT_INODE
            };
            let file = File::from_entry(self.next_inode(), entry, password.as_ref());
            self.add_or_update_file(file, parent)?;
        }
        Ok(())
    }

    fn next_inode(&mut self) -> Inode {
        self.last_inode += 1;
        self.last_inode
    }

    fn add_root_file(&mut self, file: File) -> io::Result<()> {
        self._add_file(file, InsertBehavior::AsRoot)
    }

    fn add_file(&mut self, file: File, parent: Inode) -> io::Result<()> {
        let node_id = self.node_ids.get(&parent).unwrap().clone();
        self._add_file(file, InsertBehavior::UnderNode(&node_id))
    }

    fn _add_file(&mut self, file: File, insert_behavior: InsertBehavior) -> io::Result<()> {
        let node_id = self
            .tree
            .insert(Node::new(file.attr.ino), insert_behavior)
            .map_err(|err| io::Error::other(err))?;
        self.node_ids.insert(file.attr.ino, node_id);
        self.files.insert(file.attr.ino, file);
        Ok(())
    }

    fn update_file(&mut self, ino: Inode, mut file: File) -> io::Result<()> {
        file.attr.ino = ino;
        self.files.insert(file.attr.ino, file);
        Ok(())
    }

    fn add_or_update_file(&mut self, mut file: File, parent: Inode) -> io::Result<()> {
        let children = self.get_children(parent).unwrap();
        if let Some(it) = children.iter().find(|it| it.name == file.name) {
            self.update_file(it.attr.ino, file)
        } else {
            self.add_file(file, parent)
        }
    }

    /// Create directories and return most deep directory Inode.
    fn make_dir_all(&mut self, path: &Path, mut parent: Inode) -> io::Result<Inode> {
        for component in path.components() {
            let name = component.as_os_str();
            let children = self.get_children(parent).unwrap();
            let it = children.iter().find(|it| name == it.name);
            if let Some(it) = it {
                parent = it.attr.ino;
            } else {
                let ino = self.next_inode();
                self.add_file(File::dir(ino, name.into()), parent)?;
                parent = ino;
            }
        }
        Ok(parent)
    }

    pub(crate) fn get_file(&self, ino: Inode) -> Option<&File> {
        self.files.get(&ino)
    }

    pub(crate) fn get_file_mut(&mut self, ino: Inode) -> Option<&mut File> {
        self.files.get_mut(&ino)
    }

    pub(crate) fn get_children(&self, parent: Inode) -> Option<Vec<&File>> {
        let node_id = self.node_ids.get(&parent)?;
        let children = self.tree.children(node_id).ok()?;
        children
            .map(|ino| self.files.get(ino.data()))
            .collect::<Option<Vec<_>>>()
    }
}
