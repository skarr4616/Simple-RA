#include "RecordPtr.hpp"
#include "LeafNode.hpp"

LeafNode::LeafNode(const TreePtr &tree_ptr) : TreeNode(LEAF, tree_ptr)
{
    this->data_pointers.clear();
    this->next_leaf_ptr = NULL_PTR;
    if (!is_null(tree_ptr))
        this->load();
}

// returns max key within this leaf
Key LeafNode::max()
{
    auto it = this->data_pointers.rbegin();
    return it->first;
}

// inserts <key, record_ptr> to leaf. If overflow occurs, leaf is split
// split node is returned
// TODO: LeafNode::insert_key to be implemented
TreePtr LeafNode::insert_key(const Key &key, const RecordPtr &record_ptr)
{
    TreePtr new_leaf = NULL_PTR; // if leaf is split, new_leaf = ptr to new split node ptr

    this->data_pointers[key] = record_ptr;
    this->size = this->data_pointers.size();

    if (this->overflows())
    {
        LeafNode *new_leaf_ptr = (LeafNode *)TreeNode::tree_node_factory(LEAF);

        new_leaf_ptr->parent_ptr = this->parent_ptr;
        new_leaf_ptr->next_leaf_ptr = this->next_leaf_ptr;
        this->next_leaf_ptr = new_leaf_ptr->tree_ptr;

        int new_sz = ceil((double)this->size / 2);

        map<Key, RecordPtr> temp_data_pointers = this->data_pointers;
        this->data_pointers.clear();

        int idx = 0;
        for (auto it : temp_data_pointers)
        {
            if (idx < new_sz)
                this->data_pointers[it.first] = it.second;
            else
                new_leaf_ptr->insert_key(it.first, it.second);

            idx++;
        }

        new_leaf_ptr->dump();

        new_leaf = new_leaf_ptr->tree_ptr;
        delete new_leaf_ptr;
    }

    this->size = this->data_pointers.size();

    this->dump();
    return new_leaf;
}

/* Delete functions */
void LeafNode::redistribute(TreePtr a, string type)
{
    LeafNode *a_ptr = (LeafNode *)TreeNode::tree_node_factory(a);

    map<Key, RecordPtr> temp_data_pointers = this->data_pointers;
    this->data_pointers.clear();

    for (auto it : a_ptr->data_pointers)
        temp_data_pointers[it.first] = it.second;
    a_ptr->data_pointers.clear();

    if (type == "LEFT")
    {
        // a is the left node

        int idx = 0;
        for (auto it : temp_data_pointers)
        {
            if (idx < temp_data_pointers.size() - MIN_OCCUPANCY)
                a_ptr->data_pointers[it.first] = it.second;
            else
                this->data_pointers[it.first] = it.second;

            idx++;
        }
    }
    else
    {
        // a is the right node

        int idx = 0;
        for (auto it : temp_data_pointers)
        {
            if (idx < MIN_OCCUPANCY)
                this->data_pointers[it.first] = it.second;
            else
                a_ptr->data_pointers[it.first] = it.second;

            idx++;
        }
    }

    a_ptr->size = a_ptr->data_pointers.size();
    a_ptr->dump();
    delete a_ptr;

    this->size = this->data_pointers.size();
    this->dump();
}

void LeafNode::merge(TreePtr a, string type)
{
    LeafNode *a_ptr = (LeafNode *)TreeNode::tree_node_factory(a);

    for (auto it : this->data_pointers)
        a_ptr->data_pointers[it.first] = it.second;

    a_ptr->size = a_ptr->data_pointers.size();
    a_ptr->dump();
    delete a_ptr;
}

// key is deleted from leaf if exists
// TODO: LeafNode::delete_key to be implemented
void LeafNode::delete_key(const Key &key)
{
    if (this->data_pointers.find(key) != this->data_pointers.end())
    {
        this->data_pointers.erase(this->data_pointers.find(key));
        this->size = this->data_pointers.size();
    }

    this->dump();
}

// runs range query on leaf
void LeafNode::range(ostream &os, const Key &min_key, const Key &max_key) const
{
    BLOCK_ACCESSES++;
    for (const auto &data_pointer : this->data_pointers)
    {
        if (data_pointer.first >= min_key && data_pointer.first <= max_key)
            data_pointer.second.write_data(os);
        if (data_pointer.first > max_key)
            return;
    }
    if (!is_null(this->next_leaf_ptr))
    {
        auto next_leaf_node = new LeafNode(this->next_leaf_ptr);
        next_leaf_node->range(os, min_key, max_key);
        delete next_leaf_node;
    }
}

// exports node - used for grading
void LeafNode::export_node(ostream &os)
{
    TreeNode::export_node(os);
    for (const auto &data_pointer : this->data_pointers)
    {
        os << data_pointer.first << " ";
    }
    os << endl;
}

// writes leaf as a mermaid chart
void LeafNode::chart(ostream &os)
{
    string chart_node = this->tree_ptr + "[" + this->tree_ptr + BREAK;
    chart_node += "size: " + to_string(this->size) + BREAK;
    for (const auto &data_pointer : this->data_pointers)
    {
        chart_node += to_string(data_pointer.first) + " ";
    }
    chart_node += "]";
    os << chart_node << endl;
}

ostream &LeafNode::write(ostream &os) const
{
    TreeNode::write(os);
    for (const auto &data_pointer : this->data_pointers)
    {
        if (&os == &cout)
            os << "\n"
               << data_pointer.first << ": ";
        else
            os << "\n"
               << data_pointer.first << " ";
        os << data_pointer.second;
    }
    os << endl;
    os << this->next_leaf_ptr << endl;
    return os;
}

istream &LeafNode::read(istream &is)
{
    TreeNode::read(is);
    this->data_pointers.clear();
    for (int i = 0; i < this->size; i++)
    {
        Key key = DELETE_MARKER;
        RecordPtr record_ptr;
        if (&is == &cin)
            cout << "K: ";
        is >> key;
        if (&is == &cin)
            cout << "P: ";
        is >> record_ptr;
        this->data_pointers.insert(pair<Key, RecordPtr>(key, record_ptr));
    }
    is >> this->next_leaf_ptr;
    return is;
}