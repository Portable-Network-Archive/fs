use fuser::{FileAttr, FileType};
use id_tree::{InsertBehavior, Node, NodeId, RemoveBehavior, Tree, TreeBuilder};
#[cfg(unix)]
use nix::unistd::{Gid, Group, Uid, User};
use pna::{
    Archive, DataKind, EntryName, Metadata, Permission, ReadEntry, ReadOptions, WriteOptions,
};
use std::collections::HashMap;
use std::ffi::OsString;
use std::io::{BufWriter, prelude::*};
use std::path::{Path, PathBuf};
use std::time::SystemTime;
use std::{fs, io};

pub type Inode = u64;

pub(crate) struct LoadedEntry {
    data: Vec<u8>,
    xattrs: HashMap<OsString, Vec<u8>>,
}

pub(crate) struct UnprocessedEntry {
    entry: pna::NormalEntry,
    option: ReadOptions,
}

pub(crate) enum Entry {
    Loaded(LoadedEntry),
    Unprocessed(UnprocessedEntry),
}

impl Entry {
    fn empty() -> Self {
        Self::Loaded(LoadedEntry {
            data: Vec::new(),
            xattrs: HashMap::new(),
        })
    }

    fn load(&mut self) -> &LoadedEntry {
        if let Self::Unprocessed(e) = &self {
            let mut xattrs = HashMap::with_capacity(e.entry.xattrs().len());
            for xattr in e.entry.xattrs() {
                xattrs.insert(xattr.name().into(), xattr.value().into());
            }
            let mut buf = Vec::new();
            let mut reader = e.entry.reader(e.option.clone()).unwrap();
            reader.read_to_end(&mut buf).unwrap();
            *self = Self::Loaded(LoadedEntry { data: buf, xattrs });
        }
        match self {
            Self::Loaded(l) => l,
            Self::Unprocessed(_) => unreachable!(),
        }
    }

    #[inline]
    pub(crate) fn as_slice(&mut self) -> &[u8] {
        self.load().data.as_slice()
    }

    #[inline]
    pub(crate) fn xattrs(&mut self) -> &HashMap<OsString, Vec<u8>> {
        &self.load().xattrs
    }

    #[inline]
    pub(crate) fn as_mut_slice(&mut self) -> &mut Vec<u8> {
        self.load();
        match self {
            Entry::Loaded(l) => &mut l.data,
            Entry::Unprocessed(_) => unreachable!(),
        }
    }
}

pub(crate) struct File {
    pub(crate) name: OsString,
    pub(crate) data: Entry,
    pub(crate) attr: FileAttr,
}

impl File {
    pub(crate) fn dir(inode: Inode, name: OsString) -> Self {
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
                uid: get_owner_id(None),
                gid: get_group_id(None),
                rdev: 0,
                blksize: 0,
                flags: 0,
            },
        }
    }

    #[inline]
    fn root(inode: Inode) -> Self {
        Self::dir(inode, ".".into())
    }

    fn from_entry<S: Into<String>>(
        inode: Inode,
        entry: pna::NormalEntry,
        password: Option<S>,
    ) -> Self {
        let now = SystemTime::now();
        let header = entry.header();
        let metadata = entry.metadata();
        let name = header
            .path()
            .as_path()
            .components()
            .next_back()
            .unwrap()
            .as_os_str()
            .into();

        let mut attr = FileAttr {
            ino: inode,
            size: 0,
            blocks: 1,
            atime: metadata
                .modified()
                .map_or(now, |it| SystemTime::UNIX_EPOCH + it),
            mtime: metadata
                .modified()
                .map_or(now, |it| SystemTime::UNIX_EPOCH + it),
            ctime: metadata
                .modified()
                .map_or(now, |it| SystemTime::UNIX_EPOCH + it),
            crtime: metadata
                .created()
                .map_or(now, |it| SystemTime::UNIX_EPOCH + it),
            kind: match header.data_kind() {
                DataKind::File => FileType::RegularFile,
                DataKind::Directory => FileType::Directory,
                DataKind::SymbolicLink => FileType::Symlink,
                DataKind::HardLink => FileType::RegularFile,
            },
            perm: metadata.permission().map_or(0o775, |it| it.permissions()),
            nlink: 1,
            uid: get_owner_id(metadata.permission()),
            gid: get_group_id(metadata.permission()),
            rdev: 0,
            blksize: 512,
            flags: 0,
        };
        let option = ReadOptions::with_password(password);
        let (data, raw_size) = if let Some(raw_size) = metadata.raw_file_size() {
            let data = Entry::Unprocessed(UnprocessedEntry { entry, option });
            (data, raw_size as usize)
        } else {
            let mut data = Entry::Unprocessed(UnprocessedEntry { entry, option });
            let raw_size = data.as_slice().len();
            (data, raw_size)
        };
        attr.size = raw_size as u64;
        Self { name, attr, data }
    }
}

const ROOT_INODE: Inode = 1;

pub(crate) struct FileManager {
    archive_path: PathBuf,
    tree: Tree<Inode>,
    files: HashMap<Inode, File>,
    node_ids: HashMap<Inode, NodeId>,
    last_inode: Inode,
}

impl FileManager {
    pub(crate) fn new(archive_path: PathBuf, password: Option<String>) -> Self {
        let mut manager = Self {
            archive_path,
            tree: TreeBuilder::new().build(),
            files: HashMap::new(),
            node_ids: HashMap::new(),
            last_inode: ROOT_INODE,
        };
        manager.populate(password.as_deref()).unwrap();
        manager
    }

    fn populate(&mut self, password: Option<&str>) -> io::Result<()> {
        self.add_root_file(File::root(ROOT_INODE))?;
        let file = fs::File::open(&self.archive_path)?;
        let memmap = unsafe { memmap2::Mmap::map(&file) }?;
        let mut archive = Archive::read_header_from_slice(&memmap[..])?;
        for entry in archive.entries_slice() {
            let entry = entry?;
            match entry {
                ReadEntry::Solid(s) => {
                    for entry in s.entries(password)? {
                        let entry = entry?;
                        let parents = entry.header().path().as_path().parent();
                        let parent = if let Some(parents) = parents {
                            self.make_dir_all(parents, ROOT_INODE)?
                        } else {
                            ROOT_INODE
                        };
                        let file = File::from_entry(self.next_inode(), entry, password);
                        self.add_or_update_file(file, parent)?;
                    }
                }
                ReadEntry::Normal(entry) => {
                    let parents = entry.header().path().as_path().parent();
                    let parent = if let Some(parents) = parents {
                        self.make_dir_all(parents, ROOT_INODE)?
                    } else {
                        ROOT_INODE
                    };
                    let file = File::from_entry(self.next_inode(), entry.into(), password);
                    self.add_or_update_file(file, parent)?;
                }
            }
        }
        Ok(())
    }

    fn next_inode(&mut self) -> Inode {
        self.last_inode += 1;
        self.last_inode
    }

    fn add_root_file(&mut self, file: File) -> io::Result<FileAttr> {
        self._add_file(file, InsertBehavior::AsRoot)
    }

    fn add_file(&mut self, file: File, parent: Inode) -> io::Result<FileAttr> {
        let node_id = self.node_ids.get(&parent).unwrap().clone();
        self._add_file(file, InsertBehavior::UnderNode(&node_id))
    }

    fn _add_file(&mut self, file: File, insert_behavior: InsertBehavior) -> io::Result<FileAttr> {
        let attr = file.attr;
        let node_id = self
            .tree
            .insert(Node::new(file.attr.ino), insert_behavior)
            .map_err(io::Error::other)?;
        self.node_ids.insert(file.attr.ino, node_id);
        self.files.insert(file.attr.ino, file);
        Ok(attr)
    }

    pub(crate) fn make_dir(&mut self, parent: Inode, name: OsString) -> io::Result<FileAttr> {
        let ino = self.next_inode();

        let file = File::dir(ino, name);
        let attr = file.attr;
        self.add_file(file, parent)?;
        Ok(attr)
    }

    pub(crate) fn create_file(&mut self, parent: Inode, name: OsString) -> io::Result<FileAttr> {
        let now = SystemTime::now();

        let ino = self.next_inode();

        // 新しいファイル属性を生成
        let attr = FileAttr {
            ino,
            size: 0,
            blocks: 1,
            atime: now,
            mtime: now,
            ctime: now,
            crtime: now,
            kind: FileType::RegularFile,
            perm: 0o644,
            nlink: 1,
            uid: unsafe { libc::getuid() },
            gid: unsafe { libc::getgid() },
            rdev: 0,
            blksize: 512,
            flags: 0,
        };

        // ファイル構造体を作成
        let file = File {
            name,
            attr,
            data: Entry::Loaded(LoadedEntry {
                data: Vec::new(),
                xattrs: HashMap::new(),
            }),
        };
        self.add_file(file, parent)?;
        Ok(attr)
    }

    fn update_file(&mut self, ino: Inode, mut file: File) -> io::Result<FileAttr> {
        file.attr.ino = ino;
        let attr = file.attr;
        self.files.insert(file.attr.ino, file);
        Ok(attr)
    }

    fn add_or_update_file(&mut self, file: File, parent: Inode) -> io::Result<FileAttr> {
        let children = self.get_children(parent).unwrap();
        if let Some(it) = children.iter().find(|it| it.name == file.name) {
            self.update_file(it.attr.ino, file)
        } else {
            self.add_file(file, parent)
        }
    }

    /// Create directories and return deepest directory Inode.
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

    /// 指定したinodeのファイルへの可変参照を返す。
    /// inodeが存在しない場合はNone。
    pub(crate) fn get_file_mut(&mut self, ino: Inode) -> Option<&mut File> {
        self.files.get_mut(&ino)
    }

    /// 指定した親inode直下の子ファイル一覧を返す。
    /// 親inodeが存在しない場合やツリー不整合時はNone。
    pub(crate) fn get_children(&self, parent: Inode) -> Option<Vec<&File>> {
        let node_id = self.node_ids.get(&parent)?;
        let children = self.tree.children(node_id).ok()?;
        children
            .map(|ino| self.files.get(ino.data()))
            .collect::<Option<Vec<_>>>()
    }

    /// Remove the file or directory with the specified inode.
    /// Returns false if the inode does not exist. All descendants are safely removed from the tree.
    pub(crate) fn remove_file(&mut self, ino: Inode) -> bool {
        if let Some(node_id) = self.node_ids.remove(&ino) {
            // Remove the node and all descendants from the tree
            let _ = self.tree.remove_node(node_id, RemoveBehavior::DropChildren);
            self.files.remove(&ino).is_some()
        } else {
            false
        }
    }

    /// Persist all files, directories, and symlinks to the archive as much as the current API allows.
    /// Attributes and xattrs are also persisted if supported by the API.
    pub(crate) fn save_to_archive(&self) -> io::Result<()> {
        let file = fs::File::create(&self.archive_path)?;
        let writer = BufWriter::new(file);
        let mut archive = Archive::write_header(writer)?;

        for file in self.files.values() {
            // Build the full path for the entry
            let mut full_path = vec![];
            if let Some(node_id) = self.node_ids.get(&file.attr.ino) {
                if let Ok(ancestors) = self.tree.ancestors(node_id) {
                    for ancestor in ancestors {
                        if let Some(ancestor_file) = self.files.get(ancestor.data()) {
                            full_path.push(ancestor_file.name.clone());
                        }
                    }
                }
            }
            full_path.push(file.name.clone());
            let path_str = PathBuf::from_iter(&full_path);
            let name = EntryName::from_lossy(path_str);

            // Build metadata (only default is supported by current API)
            let metadata = Metadata::default();
            // NOTE: Permission, timestamps, uid/gid, xattr are not supported by current API
            //       When the API is extended, add them here.
            let options = WriteOptions::builder().build();

            match file.attr.kind {
                FileType::RegularFile => {
                    if let Entry::Loaded(loaded) = &file.data {
                        archive.write_file(name, metadata, options, |w| {
                            w.write_all(&loaded.data)?;
                            Ok(())
                        })?;
                    }
                }
                FileType::Directory => {
                    // Directory persistence is not supported by the current archive API
                    log::warn!(
                        "Directory persistence is not supported by the current archive API. Skipped: {:?}",
                        file.name
                    );
                }
                FileType::Symlink => {
                    // Symlink persistence is not supported by the current archive API
                    log::warn!(
                        "Symlink persistence is not supported by the current archive API. Skipped: {:?}",
                        file.name
                    );
                }
                _ => {
                    log::warn!("Unsupported file type for persistence: {:?}", file.name);
                }
            }
        }

        archive.finalize()?;
        Ok(())
    }

    pub(crate) fn move_file(
        &mut self,
        ino: Inode,
        new_parent: Inode,
        new_name: OsString,
    ) -> io::Result<()> {
        use libc::ENOENT;
        // 移動元ファイルが存在するか確認
        if !self.files.contains_key(&ino) {
            return Err(io::Error::from_raw_os_error(ENOENT));
        }

        // 移動先の親ディレクトリが存在するか確認
        if !self.files.contains_key(&new_parent) {
            return Err(io::Error::from_raw_os_error(ENOENT));
        }

        // 移動先の親ディレクトリが実際にディレクトリかどうか確認
        if let Some(parent_file) = self.files.get(&new_parent) {
            if parent_file.attr.kind != FileType::Directory {
                return Err(io::Error::from_raw_os_error(libc::ENOTDIR));
            }
        }

        // 新しい親ディレクトリの下に同名のファイルが既に存在するかチェック
        if let Some(children) = self.get_children(new_parent) {
            if children.iter().any(|f| f.name == new_name) {
                return Err(io::Error::from_raw_os_error(libc::EEXIST));
            }
        }

        // ノードIDを取得
        let node_id = if let Some(id) = self.node_ids.get(&ino).cloned() {
            id
        } else {
            return Err(io::Error::from_raw_os_error(ENOENT));
        };

        // 新しい親ノードIDを取得
        let new_parent_id = if let Some(id) = self.node_ids.get(&new_parent).cloned() {
            id
        } else {
            return Err(io::Error::from_raw_os_error(ENOENT));
        };

        // Prevent moving a node under itself or its descendants (cycle check)
        // id_tree does not provide is_ancestor_of, so we check manually
        // by traversing all descendants of the node to be moved.
        let mut stack = vec![node_id.clone()];
        while let Some(current) = stack.pop() {
            if current == new_parent_id {
                return Err(io::Error::from_raw_os_error(libc::EINVAL));
            }
            if let Ok(children) = self.tree.children_ids(&current) {
                for child in children {
                    stack.push(child.clone());
                }
            }
        }

        // ファイル名を更新
        if let Some(file) = self.files.get_mut(&ino) {
            file.name = new_name;
        } else {
            return Err(io::Error::from_raw_os_error(ENOENT));
        }

        // 移動するノードとそのすべての子孫のマッピング情報を保存
        let mut node_map = HashMap::new();
        self.collect_node_subtree_mapping(&node_id, &mut node_map)?;

        // 元のノードを削除（子ノードも含めて）
        let removed_node = self
            .tree
            .remove_node(node_id, RemoveBehavior::DropChildren)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

        // 新しい親の下に移動するノードを挿入
        let new_node_id = self
            .tree
            .insert(removed_node, InsertBehavior::UnderNode(&new_parent_id))
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

        // ノードIDのマッピングを更新
        self.node_ids.insert(ino, new_node_id.clone());

        // 子孫ノードを新しい構造で再構築
        self.rebuild_subtree(new_node_id, node_map)?;

        Ok(())
    }

    // 指定されたノード以下のサブツリーのマッピング情報を収集
    fn collect_node_subtree_mapping(
        &self,
        root_id: &NodeId,
        node_map: &mut HashMap<Inode, (Inode, Vec<Inode>)>,
    ) -> io::Result<()> {
        // ルートノードのInodeを取得
        let root_inode = *self
            .tree
            .get(root_id)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?
            .data();

        // 子ノード情報を収集
        let mut children = Vec::new();

        if let Ok(child_ids) = self.tree.children_ids(root_id) {
            for child_id in child_ids {
                let child_inode = *self
                    .tree
                    .get(child_id)
                    .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?
                    .data();

                children.push(child_inode);

                // 再帰的に子ノードのサブツリーも収集
                self.collect_node_subtree_mapping(child_id, node_map)?;
            }
        }

        // 親子関係の情報を保存
        node_map.insert(root_inode, (root_inode, children));

        Ok(())
    }

    // 保存したマッピング情報を使ってサブツリーを再構築
    fn rebuild_subtree(
        &mut self,
        parent_id: NodeId,
        node_map: HashMap<Inode, (Inode, Vec<Inode>)>,
    ) -> io::Result<()> {
        let parent_inode = *self
            .tree
            .get(&parent_id)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?
            .data();

        // 親ノードの直接の子ノードを処理
        if let Some((_, children)) = node_map.get(&parent_inode) {
            for &child_inode in children {
                // 子ノードを親の下に再挿入
                let child_node = Node::new(child_inode);
                let new_child_id = self
                    .tree
                    .insert(child_node, InsertBehavior::UnderNode(&parent_id))
                    .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

                // ノードIDのマッピングを更新
                self.node_ids.insert(child_inode, new_child_id.clone());

                // 再帰的に子ノードのサブツリーも再構築
                self.rebuild_subtree(new_child_id, node_map.clone())?;
            }
        }

        Ok(())
    }
}

#[cfg(unix)]
fn search_owner(name: &str, id: u64) -> Option<User> {
    let user = User::from_name(name).ok().flatten();
    if user.is_some() {
        return user;
    }
    User::from_uid((id as u32).into()).ok().flatten()
}

#[cfg(unix)]
fn search_group(name: &str, id: u64) -> Option<Group> {
    let group = Group::from_name(name).ok().flatten();
    if group.is_some() {
        return group;
    }
    Group::from_gid((id as u32).into()).ok().flatten()
}

#[cfg(unix)]
fn get_owner_id(permission: Option<&Permission>) -> u32 {
    permission
        .and_then(|it| search_owner(it.uname(), it.uid()))
        .map_or_else(Uid::current, |it| it.uid)
        .as_raw()
}

#[cfg(unix)]
fn get_group_id(permission: Option<&Permission>) -> u32 {
    permission
        .and_then(|it| search_group(it.gname(), it.gid()))
        .map_or_else(Gid::current, |it| it.gid)
        .as_raw()
}

#[cfg(not(unix))]
fn get_owner_id(_permission: Option<&Permission>) -> u32 {
    0
}

#[cfg(not(unix))]
fn get_group_id(_permission: Option<&Permission>) -> u32 {
    0
}
