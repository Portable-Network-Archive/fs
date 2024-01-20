use fuser::{FileAttr, FileType};
use id_tree::{InsertBehavior, Node, NodeId, Tree, TreeBuilder};
use pna::{Archive, DataKind};
use std::collections::HashMap;
use std::ops::Add;
use std::path::PathBuf;
use std::time::SystemTime;
use std::{fs, io};

pub type Inode = u64;

pub(crate) struct File {
    pub(crate) name: String,
    pub(crate) attr: FileAttr,
}

impl File {
    fn dir(inode: Inode, name: String) -> Self {
        let now = SystemTime::now();
        Self {
            name,
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

    fn from_entry(inode: Inode, entry: pna::RegularEntry) -> Self {
        let now = SystemTime::now();
        let header = entry.header();
        let metadata = entry.metadata();
        Self {
            name: header
                .path()
                .as_path()
                .components()
                .last()
                .unwrap()
                .as_os_str()
                .to_string_lossy()
                .into(),
            attr: FileAttr {
                ino: inode,
                size: metadata.compressed_size() as u64,
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
            files: HashMap::with_capacity(0),
            node_ids: HashMap::with_capacity(0),
            last_inode: ROOT_INODE,
        };
        mamager.populate().unwrap();
        mamager
    }

    fn populate(&mut self) -> io::Result<()> {
        self.add_root_file(File::root(ROOT_INODE)).unwrap();
        let file = fs::File::open(&self.archive_path).unwrap();
        let mut archive = Archive::read_header(file).unwrap();
        let password = self.password.clone();
        for entry in archive.entries_with_password(password.as_deref()) {
            let entry = entry.unwrap();
            let mut parents = entry
                .header()
                .path()
                .as_path()
                .components()
                .collect::<Vec<_>>();
            parents.pop();
            let mut parent = ROOT_INODE;
            for component in parents {
                let name = component.as_os_str().to_string_lossy().to_string();
                let children = self.get_children(parent).unwrap();
                let it = children.iter().find(|it| name == it.name);
                if let Some(it) = it {
                    parent = it.attr.ino;
                } else {
                    let ino = self.next_inode();
                    self.add_file(File::dir(ino, name), parent)?;
                    parent = ino;
                }
            }
            let file = File::from_entry(self.next_inode(), entry);
            self.add_file(file, parent)?;
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

    pub(crate) fn get_file(&self, ino: Inode) -> Option<&File> {
        self.files.get(&ino)
    }

    pub(crate) fn get_children(&self, parent: Inode) -> Option<Vec<&File>> {
        let node_id = self.node_ids.get(&parent)?;
        let children = self.tree.children(node_id).ok()?;
        children
            .map(|ino| self.files.get(ino.data()))
            .collect::<Option<Vec<_>>>()
    }
}
