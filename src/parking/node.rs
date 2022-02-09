use core::{
    cell::Cell,
    marker::{PhantomData, PhantomPinned},
    num::NonZeroUsize,
    pin::Pin,
    ptr::NonNull,
};

#[derive(Default)]
pub struct Node {
    key: Cell<usize>,
    ticket: Cell<usize>,
    parent: Cell<Option<NonNull<Self>>>,
    children: [Cell<Option<NonNull<Self>>>; 2],
    prev: Cell<Option<NonNull<Self>>>,
    next: Cell<Option<NonNull<Self>>>,
    tail: Cell<Option<NonNull<Self>>>,
    is_inserted: Cell<bool>,
    _pinned: PhantomPinned,
}

#[derive(Default)]
pub struct Stack {
    top: Option<NonNull<Node>>,
}

impl Stack {
    pub unsafe fn push(&mut self, node: NonNull<Node>) {
        node.as_ref().next.set(self.top);
        self.top = Some(node);
    }

    pub unsafe fn pop(&mut self) -> Option<NonNull<Node>> {
        let node = self.top.take()?;
        self.top = node.as_ref().next.get();
        Some(node)
    }
}

struct Prng {
    xorshift: NonZeroUsize,
}

impl Prng {
    fn new(seed: usize) -> Self {
        Self {
            xorshift: NonZeroUsize::new(seed | 1).unwrap(),
        }
    }

    fn gen(&mut self) -> usize {
        let shifts = match usize::BITS {
            64 => (13, 7, 17),
            32 => (13, 17, 5),
            _ => unreachable!(),
        };

        let mut xs = self.xorshift.get();
        xs ^= xs << shifts.0;
        xs ^= xs >> shifts.1;
        xs ^= xs << shifts.2;

        self.xorshift = NonZeroUsize::new(xs).unwrap();
        xs
    }
}

#[derive(Default)]
pub struct Tree {
    root: Option<NonNull<Node>>,
    prng: Option<Prng>,
}

unsafe impl Send for Tree {}

impl Tree {
    pub const fn new() -> Self {
        Self {
            root: None,
            prng: None,
        }
    }

    pub fn find(&mut self, key: usize) -> List<'_> {
        let mut parent = None;
        let mut head = self.root;

        while let Some(node) = head {
            unsafe {
                let node_key = node.as_ref().key.get();
                if node_key == key {
                    break;
                }

                parent = head;
                let is_right = key > node_key;
                head = node.as_ref().children[is_right as usize].get();
            }
        }

        List {
            key: key,
            tree: self,
            head,
            parent,
        }
    }

    unsafe fn insert(&mut self, node: NonNull<Node>, key: usize, parent: Option<NonNull<Node>>) {
        // ensure pseudo random number generator is initialized
        if self.prng.is_none() {
            let seed = node.as_ptr() as usize;
            self.prng = Some(Prng::new(seed));
        }

        // generate a randomized ticket (priority) for the node
        let node_ticket = self.prng.as_mut().unwrap().gen();
        node.as_ref().ticket.set(node_ticket);

        // set the node fields
        node.as_ref().key.set(key);
        node.as_ref().parent.set(parent);
        node.as_ref().children[0].set(None);
        node.as_ref().children[1].set(None);

        // attach the node to it's parent
        match parent {
            Some(parent) => {
                let parent_key = parent.as_ref().key.get();
                let node_key = node.as_ref().key.get();
                assert_ne!(node_key, parent_key);

                let is_right = node_key > parent_key;
                let link = &parent.as_ref().children[is_right as usize];
                assert_eq!(link.replace(Some(node)), None);
            }
            None => {
                assert_eq!(self.root, None);
                self.root = Some(node);
            }
        }

        // rotate the node up into the tree according to its ticket (priority)
        while let Some(parent) = node.as_ref().parent.get() {
            let parent_ticket = parent.as_ref().ticket.get();
            if parent_ticket <= node_ticket {
                break;
            }

            let is_right = parent.as_ref().children[1].get() == Some(node);
            if !is_right {
                assert_eq!(parent.as_ref().children[0].get(), Some(node));
            }

            let opposite_direction = !is_right;
            self.rotate(opposite_direction, parent);
        }
    }

    unsafe fn remove(&mut self, node: NonNull<Node>) {
        // rotate the node down to a leaf for removal while respecting priorities
        loop {
            let right = node.as_ref().children[1].get();
            let left = node.as_ref().children[0].get();
            if left.is_none() && right.is_none() {
                break;
            }

            let left_ticket = left.map(|n| n.as_ref().ticket.get());
            let right_ticket = right.map(|n| n.as_ref().ticket.get());

            let is_right = match (left_ticket, right_ticket) {
                (Some(left_ticket), Some(right_ticket)) => left_ticket < right_ticket,
                (_, None) => true,
                _ => false,
            };

            self.rotate(is_right, node);
        }

        // then remove the node now that it's a leaf
        self.replace(node, None);
        node.as_ref().key.set(0);
        node.as_ref().ticket.set(0);
        node.as_ref().parent.set(None);
        node.as_ref().children[0].set(None);
        node.as_ref().children[1].set(None);
    }

    unsafe fn replace(&mut self, old_node: NonNull<Node>, new_node: Option<NonNull<Node>>) {
        // set the new_node's fields
        if let Some(new_node) = new_node {
            new_node.as_ref().key.set(old_node.as_ref().key.get());
            new_node.as_ref().ticket.set(old_node.as_ref().ticket.get());
            new_node.as_ref().parent.set(old_node.as_ref().parent.get());
            new_node.as_ref().children[0].set(old_node.as_ref().children[0].get());
            new_node.as_ref().children[1].set(old_node.as_ref().children[1].get());
        }

        // make the parent's child point to the new_node
        let parent = old_node.as_ref().parent.get();
        self.update_parent(parent, old_node, new_node);

        // make the node's children point to the new_node as the parent
        for (index, link) in old_node.as_ref().children.iter().enumerate() {
            if let Some(child) = link.get() {
                let node_key = old_node.as_ref().key.get();
                let child_key = child.as_ref().key.get();
                assert_ne!(node_key, child_key);

                let is_right = index == 1;
                if is_right {
                    assert!(child_key > node_key);
                } else {
                    assert!(child_key < node_key);
                }

                let old_parent = child.as_ref().parent.replace(new_node);
                assert_eq!(old_parent, Some(old_node));
            }
        }
    }

    unsafe fn rotate(&mut self, is_right: bool, node: NonNull<Node>) {
        let parent = node.as_ref().parent.get();
        let target = node.as_ref().children[(!is_right) as usize].get().unwrap();
        let adjacent = node.as_ref().children[is_right as usize].get();

        target.as_ref().children[is_right as usize].set(Some(node));
        node.as_ref().parent.set(Some(target));

        node.as_ref().children[(!is_right) as usize].set(adjacent);
        if let Some(adjacent) = adjacent {
            adjacent.as_ref().parent.set(Some(node));
        }

        target.as_ref().parent.set(parent);
        self.update_parent(parent, node, Some(target));
    }

    unsafe fn update_parent(
        &mut self,
        parent: Option<NonNull<Node>>,
        old_node: NonNull<Node>,
        new_node: Option<NonNull<Node>>,
    ) {
        match parent {
            Some(parent) => {
                let is_right = parent.as_ref().children[1].get() == Some(old_node);
                let link = &parent.as_ref().children[is_right as usize];
                assert_eq!(link.replace(new_node), Some(old_node));
            }
            None => {
                assert_eq!(self.root, Some(old_node));
                self.root = new_node;
            }
        }
    }
}

pub struct List<'a> {
    key: usize,
    tree: &'a mut Tree,
    head: Option<NonNull<Node>>,
    parent: Option<NonNull<Node>>,
}

impl<'a> List<'a> {
    pub fn iter(&self) -> impl Iterator<Item = Pin<&'a Node>> + 'a {
        struct NodeIter<'a> {
            node: Option<NonNull<Node>>,
            _lifetime: PhantomData<Pin<&'a Node>>,
        }

        impl<'a> Iterator for NodeIter<'a> {
            type Item = Pin<&'a Node>;

            fn next(&mut self) -> Option<Self::Item> {
                // safety: see List::insert()
                unsafe {
                    let node = self.node?;
                    self.node = node.as_ref().next.get();
                    Some(Pin::new_unchecked(&*node.as_ptr()))
                }
            }
        }

        NodeIter {
            node: self.head,
            _lifetime: PhantomData,
        }
    }

    /// Inserts the node into the List for this key.
    ///
    /// # Safety
    ///
    /// The caller must ensure the Node remains valid and Pinned
    /// until try_remove() on the same conceptual List is called
    /// and returns true.
    pub unsafe fn insert(&mut self, node: Pin<&Node>) {
        let node = Pin::into_inner_unchecked(node);
        let node = NonNull::from(node);

        // initialize the node
        node.as_ref().prev.set(None);
        node.as_ref().next.set(None);
        node.as_ref().tail.set(None);
        node.as_ref().is_inserted.set(true);

        // push to the existing queue of nodes
        if let Some(head) = self.head {
            let tail = head.as_ref().tail.get().unwrap();
            tail.as_ref().next.set(Some(node));
            node.as_ref().prev.set(Some(tail));
            head.as_ref().tail.set(Some(node));
            return;
        }

        // this is the first node in the queue so insert it into the tree
        self.head = Some(node);
        node.as_ref().tail.set(Some(node));
        self.tree.insert(node, self.key, self.parent);
    }

    /// Tries to remove the node from the List associated with its key.
    /// Returns true if the node was successfully removed and can be invalidated.
    /// Returns false if the node has already been removed.
    ///
    /// # Safety
    ///
    /// The caller must ensure that the node has been `insert`ed before hand
    /// into the same conceptual List.
    pub unsafe fn try_remove(&mut self, node: Pin<&Node>) -> bool {
        let node = Pin::into_inner_unchecked(node);
        let node = NonNull::from(node);

        if !node.as_ref().is_inserted.replace(false) {
            return false;
        }

        self.remove(node);
        true
    }

    unsafe fn remove(&mut self, node: NonNull<Node>) {
        // read the node and queue links upfront
        let head = self.head.unwrap();
        let tail = head.as_ref().tail.get().unwrap();
        let prev = node.as_ref().prev.get();
        let next = node.as_ref().next.get();

        // a node with a previous link is guaranteed not to be the head
        if let Some(prev) = prev {
            assert_ne!(head, node);
            prev.as_ref().next.set(next);

            // a node with both prev and next is guaranteed to be in the middle and not the tail
            if let Some(next) = next {
                assert_ne!(tail, node);
                next.as_ref().prev.set(Some(prev));
                return;
            }

            assert_eq!(tail, node);
            head.as_ref().tail.set(Some(prev));
            return;
        }

        // the node is at the head of the queue
        assert_eq!(head, node);
        self.head = next;

        // update references to/for the new head
        // by giving it its tail to track and updating the tree reference.
        match self.head {
            None => self.tree.remove(head),
            Some(new_head) => {
                let old_prev = new_head.as_ref().prev.replace(None);
                assert_eq!(old_prev, Some(head));

                new_head.as_ref().tail.set(Some(tail));
                self.tree.replace(head, Some(new_head));
            }
        }
    }
}
