#include "InternalNode.hpp"

// creates internal node pointed to by tree_ptr or creates a new one
InternalNode::InternalNode(const TreePtr &tree_ptr) : TreeNode(INTERNAL, tree_ptr)
{
    this->keys.clear();
    this->tree_pointers.clear();
    if (!is_null(tree_ptr))
        this->load();
}

// max element from tree rooted at this node
Key InternalNode::max()
{
    Key max_key = DELETE_MARKER;
    TreeNode *last_tree_node = TreeNode::tree_node_factory(this->tree_pointers[this->size - 1]);
    max_key = last_tree_node->max();
    delete last_tree_node;
    return max_key;
}

// if internal node contains a single child, it is returned
TreePtr InternalNode::single_child_ptr()
{
    if (this->size == 1)
    {
        auto ptr = TreeNode::tree_node_factory(this->tree_pointers[0]);
        ptr->parent_ptr = NULL_PTR;
        ptr->dump();
        delete ptr;

        return this->tree_pointers[0];
    }
    return NULL_PTR;
}

// inserts <key, record_ptr> into subtree rooted at this node.
// returns pointer to split node if exists
// TODO: InternalNode::insert_key to be implemented
TreePtr InternalNode::insert_key(const Key &key, const RecordPtr &record_ptr)
{
    TreePtr new_tree_ptr = NULL_PTR;

    // Getting the idx of the tree pointer
    auto it = lower_bound(this->keys.begin(), this->keys.end(), key);
    unsigned int idx = it - this->keys.begin();

    // Getting the tree pointer
    TreeNode *node = TreeNode::tree_node_factory(this->tree_pointers[idx]);

    // Inserting the key
    TreePtr potential_split_node_ptr = node->insert_key(key, record_ptr);

    // If internal node is to be split
    if (!is_null(potential_split_node_ptr))
    {
        TreeNode *right_node = TreeNode::tree_node_factory(potential_split_node_ptr);

        Key left_child_key = node->max();
        Key right_child_key = right_node->max();

        delete right_node;

        if (idx != this->keys.size())
        {
            this->keys[idx] = left_child_key;
            this->keys.insert(this->keys.begin() + idx + 1, right_child_key);
        }
        else
        {
            this->keys.insert(this->keys.begin() + idx, left_child_key);
        }

        this->tree_pointers.insert(this->tree_pointers.begin() + idx + 1, potential_split_node_ptr);
    }

    delete node;
    this->size = this->tree_pointers.size();

    if (this->overflows())
    {
        InternalNode *new_internal_node = (InternalNode *)TreeNode::tree_node_factory(INTERNAL);
        new_internal_node->parent_ptr = this->parent_ptr;

        int new_sz = ceil((double)this->size / 2);

        vector<Key> temp_keys = this->keys;
        vector<TreePtr> temp_tree_pointers = this->tree_pointers;

        this->keys.clear();
        this->tree_pointers.clear();

        for (int i = 0; i < this->size - 1; i++)
        {
            if (i < new_sz)
            {
                this->keys.push_back(temp_keys[i]);
                this->tree_pointers.push_back(temp_tree_pointers[i]);
            }
            else
            {
                new_internal_node->keys.push_back(temp_keys[i]);
                new_internal_node->tree_pointers.push_back(temp_tree_pointers[i]);

                auto curr_node_ptr = TreeNode::tree_node_factory(temp_tree_pointers[i]);
                curr_node_ptr->parent_ptr = new_internal_node->tree_ptr;
                curr_node_ptr->dump();
                delete curr_node_ptr;
            }
        }

        new_internal_node->tree_pointers.push_back(temp_tree_pointers[this->size - 1]);

        auto curr_node_ptr = TreeNode::tree_node_factory(temp_tree_pointers[this->size - 1]);
        curr_node_ptr->parent_ptr = new_internal_node->tree_ptr;
        curr_node_ptr->dump();
        delete curr_node_ptr;

        new_internal_node->size = new_internal_node->tree_pointers.size();
        new_internal_node->dump();

        new_tree_ptr = new_internal_node->tree_ptr;
        delete new_internal_node;
    }

    this->size = this->tree_pointers.size();

    this->dump();
    return new_tree_ptr;
}

/* Delete Functions */
TreePtr InternalNode::getLeftSibling(const Key &key)
{
    // Getting the idx of the tree pointer
    auto it = lower_bound(this->keys.begin(), this->keys.end(), key);
    unsigned int idx = it - this->keys.begin();

    if (idx != 0)
        return this->tree_pointers[idx - 1];

    if (is_null(this->parent_ptr))
        return NULL_PTR;

    InternalNode *parent_node_ptr = (InternalNode *)TreeNode::tree_node_factory(this->parent_ptr);
    TreePtr left_sibling = parent_node_ptr->getLeftSibling(key);
    delete parent_node_ptr;

    if (is_null(left_sibling))
        return NULL_PTR;

    InternalNode *left_sibling_ptr = (InternalNode *)TreeNode::tree_node_factory(left_sibling);
    left_sibling = left_sibling_ptr->tree_pointers.back();
    delete left_sibling_ptr;

    return left_sibling;
}

TreePtr InternalNode::getRightSibling(const Key &key)
{
    // Getting the idx of the tree pointer
    auto it = lower_bound(this->keys.begin(), this->keys.end(), key);
    unsigned int idx = it - this->keys.begin();

    if (idx != this->size - 1)
        return this->tree_pointers[idx + 1];

    if (is_null(this->parent_ptr))
        return NULL_PTR;

    InternalNode *parent_node_ptr = (InternalNode *)TreeNode::tree_node_factory(this->parent_ptr);
    TreePtr right_sibling = parent_node_ptr->getRightSibling(key);
    delete parent_node_ptr;

    if (is_null(right_sibling))
        return NULL_PTR;

    InternalNode *right_sibling_ptr = (InternalNode *)TreeNode::tree_node_factory(right_sibling);
    right_sibling = right_sibling_ptr->tree_pointers[0];
    delete right_sibling_ptr;

    return right_sibling;
}

bool InternalNode::canRedistribute(TreePtr a, TreePtr b)
{
    // a is redistributed with b
    TreeNode *a_ptr = TreeNode::tree_node_factory(a);
    TreeNode *b_ptr = TreeNode::tree_node_factory(b);

    if (b_ptr->size - (MIN_OCCUPANCY - a_ptr->size) >= MIN_OCCUPANCY)
    {
        delete a_ptr;
        delete b_ptr;

        return true;
    }

    delete a_ptr;
    delete b_ptr;

    return false;
}

int InternalNode::updateTree(TreePtr parent_ptr)
{
    this->keys.clear();
    for (int i = 0; i < this->size - 1; i++)
    {
        TreeNode *ptr = TreeNode::tree_node_factory(this->tree_pointers[i]);

        if (ptr->node_type == LEAF)
        {
            this->keys.push_back(ptr->max());
            ptr->parent_ptr = this->tree_ptr;
            ptr->dump();
        }
        else
            this->keys.push_back(((InternalNode *)ptr)->updateTree(this->tree_ptr));

        delete ptr;
    }

    this->parent_ptr = parent_ptr;
    this->dump();

    TreeNode *ptr = TreeNode::tree_node_factory(this->tree_pointers.back());

    if (ptr->node_type == LEAF)
    {
        ptr->parent_ptr = this->tree_ptr;
        ptr->dump();
        int maxVal = ptr->max();
        delete ptr;

        return maxVal;
    }

    int maxVal = ((InternalNode *)ptr)->updateTree(this->tree_ptr);
    delete ptr;

    return maxVal;
}

void InternalNode::redistribute(TreePtr a, string type)
{
    InternalNode *a_ptr = (InternalNode *)TreeNode::tree_node_factory(a);

    if (type == "LEFT")
    {
        // a is the left node
        vector<TreePtr> temp_tree_pointers = a_ptr->tree_pointers;
        for (auto ptr : this->tree_pointers)
            temp_tree_pointers.push_back(ptr);

        a_ptr->tree_pointers.clear();
        this->tree_pointers.clear();

        int idx = 0;
        for (auto ptr : temp_tree_pointers)
        {
            if (idx < temp_tree_pointers.size() - MIN_OCCUPANCY)
                a_ptr->tree_pointers.push_back(ptr);
            else
                this->tree_pointers.push_back(ptr);

            idx++;
        }
    }
    else
    {
        // a is the right node
        vector<TreePtr> temp_tree_pointers = this->tree_pointers;
        for (auto ptr : a_ptr->tree_pointers)
            temp_tree_pointers.push_back(ptr);

        a_ptr->tree_pointers.clear();
        this->tree_pointers.clear();

        int idx = 0;
        for (auto ptr : temp_tree_pointers)
        {
            if (idx < MIN_OCCUPANCY)
                this->tree_pointers.push_back(ptr);
            else
                a_ptr->tree_pointers.push_back(ptr);

            idx++;
        }
    }

    a_ptr->size = a_ptr->tree_pointers.size();
    a_ptr->dump();
    delete a_ptr;

    this->size = this->tree_pointers.size();
    this->dump();
}

void InternalNode::merge(TreePtr a, string type)
{
    InternalNode *a_ptr = (InternalNode *)TreeNode::tree_node_factory(a);

    if (type == "LEFT")
    {
        for (auto ptr : this->tree_pointers)
            a_ptr->tree_pointers.push_back(ptr);
    }
    else
    {
        for (auto ptr : a_ptr->tree_pointers)
            this->tree_pointers.push_back(ptr);

        a_ptr->tree_pointers = this->tree_pointers;
    }

    a_ptr->size = a_ptr->tree_pointers.size();
    a_ptr->dump();
    delete a_ptr;
}

// deletes key from subtree rooted at this if exists
// TODO: InternalNode::delete_key to be implemented
void InternalNode::delete_key(const Key &key)
{
    TreePtr new_tree_ptr = NULL_PTR;

    // Getting the idx of the tree pointer
    auto it = lower_bound(this->keys.begin(), this->keys.end(), key);
    unsigned int idx = it - this->keys.begin();

    // Getting the tree pointer
    TreePtr node_ptr = this->tree_pointers[idx];
    TreeNode *node = TreeNode::tree_node_factory(node_ptr);

    // Deleting the key from the node
    node->delete_key(key);

    // Handling Underflows for left child only
    if (node->underflows())
    {
        TreePtr left_node_ptr = this->getLeftSibling(key);

        if (!is_null(left_node_ptr))
        {
            if (this->canRedistribute(node_ptr, left_node_ptr))
            {
                node->redistribute(left_node_ptr, "LEFT");
            }
            else
            {
                node->merge(left_node_ptr, "LEFT");
                this->tree_pointers.erase(this->tree_pointers.begin() + idx);
                node->delete_node();
            }
        }
        else
        {
            TreePtr right_node_ptr = this->getRightSibling(key);

            if (this->canRedistribute(node_ptr, right_node_ptr))
            {
                node->redistribute(right_node_ptr, "RIGHT");
            }
            else
            {
                node->merge(right_node_ptr, "RIGHT");
                this->tree_pointers.erase(this->tree_pointers.begin() + idx);
                node->delete_node();
            }
        }
    }

    delete node;

    this->size = this->tree_pointers.size();
    this->dump();

    if (is_null(this->parent_ptr))
        this->updateTree();
}

// runs range query on subtree rooted at this node
void InternalNode::range(ostream &os, const Key &min_key, const Key &max_key) const
{
    BLOCK_ACCESSES++;
    for (int i = 0; i < this->size - 1; i++)
    {
        if (min_key <= this->keys[i])
        {
            auto *child_node = TreeNode::tree_node_factory(this->tree_pointers[i]);
            child_node->range(os, min_key, max_key);
            delete child_node;
            return;
        }
    }
    auto *child_node = TreeNode::tree_node_factory(this->tree_pointers[this->size - 1]);
    child_node->range(os, min_key, max_key);
    delete child_node;
}

// exports node - used for grading
void InternalNode::export_node(ostream &os)
{
    TreeNode::export_node(os);
    for (int i = 0; i < this->size - 1; i++)
        os << this->keys[i] << " ";
    os << endl;
    for (int i = 0; i < this->size; i++)
    {
        auto child_node = TreeNode::tree_node_factory(this->tree_pointers[i]);
        child_node->export_node(os);
        delete child_node;
    }
}

// writes subtree rooted at this node as a mermaid chart
void InternalNode::chart(ostream &os)
{
    string chart_node = this->tree_ptr + "[" + this->tree_ptr + BREAK;
    chart_node += "size: " + to_string(this->size) + BREAK;
    chart_node += "]";
    os << chart_node << endl;

    for (int i = 0; i < this->size; i++)
    {
        auto tree_node = TreeNode::tree_node_factory(this->tree_pointers[i]);
        tree_node->chart(os);
        delete tree_node;
        string link = this->tree_ptr + "-->|";

        if (i == 0)
            link += "x <= " + to_string(this->keys[i]);
        else if (i == this->size - 1)
        {
            link += to_string(this->keys[i - 1]) + " < x";
        }
        else
        {
            link += to_string(this->keys[i - 1]) + " < x <= " + to_string(this->keys[i]);
        }
        link += "|" + this->tree_pointers[i];
        os << link << endl;
    }
}

ostream &InternalNode::write(ostream &os) const
{
    TreeNode::write(os);
    for (int i = 0; i < this->size - 1; i++)
    {
        if (&os == &cout)
            os << "\nP" << i + 1 << ": ";
        os << this->tree_pointers[i] << " ";
        if (&os == &cout)
            os << "\nK" << i + 1 << ": ";
        os << this->keys[i] << " ";
    }
    if (&os == &cout)
        os << "\nP" << this->size << ": ";
    os << this->tree_pointers[this->size - 1];
    return os;
}

istream &InternalNode::read(istream &is)
{
    TreeNode::read(is);
    this->keys.assign(this->size - 1, DELETE_MARKER);
    this->tree_pointers.assign(this->size, NULL_PTR);
    for (int i = 0; i < this->size - 1; i++)
    {
        if (&is == &cin)
            cout << "P" << i + 1 << ": ";
        is >> this->tree_pointers[i];
        if (&is == &cin)
            cout << "K" << i + 1 << ": ";
        is >> this->keys[i];
    }
    if (&is == &cin)
        cout << "P" << this->size;
    is >> this->tree_pointers[this->size - 1];
    return is;
}
