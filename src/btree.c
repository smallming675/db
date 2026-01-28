#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "db.h"
#include "table.h"

#define BTREE_DEFAULT_ORDER 4

BTreeNode *btree_create_node(int order, bool is_leaf) {
    BTreeNode *node = malloc(sizeof(BTreeNode));
    if (!node)
        return NULL;

    node->is_leaf = is_leaf;
    node->key_count = 0;
    node->parent = NULL;

    int max_keys = 2 * order - 1;
    node->keys = calloc((size_t)max_keys, sizeof(Value));
    node->row_indices = calloc((size_t)max_keys, sizeof(int));

    if (!is_leaf) {
        node->children = calloc((size_t)(2 * order), sizeof(BTreeNode *));
    } else {
        node->children = NULL;
    }

    if (!node->keys || !node->row_indices || (!is_leaf && !node->children)) {
        free(node->children);
        free(node->keys);
        free(node->row_indices);
        free(node);
        return NULL;
    }

    return node;
}

void btree_free_node(BTreeNode *node, int order) {
    if (!node)
        return;

    for (int i = 0; i < node->key_count; i++) {
        free_value(&node->keys[i]);
    }

    if (!node->is_leaf && node->children) {
        for (int i = 0; i <= node->key_count; i++) {
            if (node->children[i]) {
                btree_free_node(node->children[i], order);
            }
        }
        free(node->children);
    }

    free(node->keys);
    free(node->row_indices);
    free(node);
}

static void btree_split_child(BTreeNode *parent, int child_idx, int order) {
    BTreeNode *child = parent->children[child_idx];
    BTreeNode *new_node = btree_create_node(order, child->is_leaf);
    if (!new_node) {
        return;
    }

    int mid = order - 1;
    int max_keys = 2 * order - 1;

    if (child->key_count != max_keys) {
        btree_free_node(new_node, order);
        return;
    }

    new_node->key_count = mid;

    for (int i = 0; i < mid && (i + mid + 1) < max_keys; i++) {
        new_node->keys[i] = child->keys[i + mid + 1];
        new_node->row_indices[i] = child->row_indices[i + mid + 1];
    }

    if (!child->is_leaf && child->children) {
        int max_children = 2 * order;
        for (int i = 0; i <= mid && (i + mid + 1) < max_children; i++) {
            new_node->children[i] = child->children[i + mid + 1];
        }
    }

    child->key_count = mid;

    for (int i = parent->key_count; i > child_idx; i--) {
        parent->children[i + 1] = parent->children[i];
    }
    parent->children[child_idx + 1] = new_node;

    for (int i = parent->key_count - 1; i >= child_idx; i--) {
        parent->keys[i + 1] = parent->keys[i];
        parent->row_indices[i + 1] = parent->row_indices[i];
    }

    if (mid < max_keys) {
        parent->keys[child_idx] = child->keys[mid];
        parent->row_indices[child_idx] = child->row_indices[mid];
    }
    parent->key_count++;

    new_node->parent = parent;
    child->parent = parent;
}

static bool btree_insert_nonfull(BTreeNode *node, const Value *key, int row_index, int order) {
    int max_keys = 2 * order - 1;

    if (node->key_count >= max_keys) {
        return false;
    }

    if (node->is_leaf) {
        int insert_pos = node->key_count;
        while (insert_pos > 0 && compare_values(key, &node->keys[insert_pos - 1]) < 0) {
            node->keys[insert_pos] = node->keys[insert_pos - 1];
            node->row_indices[insert_pos] = node->row_indices[insert_pos - 1];
            insert_pos--;
        }

        node->keys[insert_pos] = copy_value(key);
        node->row_indices[insert_pos] = row_index;
        node->key_count++;
        return true;
    } else {
        int i = node->key_count - 1;
        while (i >= 0 && compare_values(key, &node->keys[i]) < 0) {
            i--;
        }
        i++;

        if (node->children[i]->key_count == 2 * order - 1) {
            btree_split_child(node, i, order);
            if (compare_values(key, &node->keys[i]) > 0) {
                i++;
            }
        }

        return btree_insert_nonfull(node->children[i], key, row_index, order);
    }
}

bool btree_insert(Index *index, const Value *key, int row_index) {
    if (index->type != INDEX_TYPE_BTREE)
        return false;

    BTreeNode *root = index->data.btree.root;
    int order = index->data.btree.order;

    if (!root) {
        root = btree_create_node(order, true);
        if (!root) {
            return false;
        }
        index->data.btree.root = root;
    }

    if (root->key_count == 2 * order - 1) {
        BTreeNode *new_root = btree_create_node(order, false);
        if (!new_root) {
            return false;
        }
        new_root->children[0] = root;
        root->parent = new_root;
        index->data.btree.root = new_root;

        btree_split_child(new_root, 0, order);
        return btree_insert_nonfull(new_root, key, row_index, order);
    } else {
        return btree_insert_nonfull(root, key, row_index, order);
    }

    return true;
}

static void btree_search_range(BTreeNode *node, const Value *min_key, const Value *max_key,
                               ArrayList *results) {
    if (node->is_leaf) {
        for (int i = 0; i < node->key_count; i++) {
            if ((!min_key || compare_values(&node->keys[i], min_key) >= 0) &&
                (!max_key || compare_values(&node->keys[i], max_key) <= 0)) {
                int *row_idx = (int *)alist_append(results);
                if (row_idx)
                    *row_idx = node->row_indices[i];
            }
        }
    } else {
        for (int i = 0; i <= node->key_count; i++) {
            if (i < node->key_count && min_key && compare_values(&node->keys[i], min_key) < 0) {
                continue;
            }
            if (i > 0 && max_key && compare_values(&node->keys[i - 1], max_key) > 0) {
                break;
            }
            btree_search_range(node->children[i], min_key, max_key, results);
        }
    }
}

ArrayList *btree_find_range(Index *index, const Value *min_key, const Value *max_key) {
    if (index->type != INDEX_TYPE_BTREE || !index->data.btree.root)
        return NULL;

    ArrayList *results = malloc(sizeof(ArrayList));
    if (!results) {
        return NULL;
    }
    alist_init(results, sizeof(int), NULL);
    if (!results->data) {
        free(results);
        return NULL;
    }

    btree_search_range(index->data.btree.root, min_key, max_key, results);
    return results;
}

ArrayList *btree_find_equals(Index *index, const Value *key) {
    return btree_find_range(index, key, key);
}
