#ifndef ALIST_H
#define ALIST_H

#include <stdbool.h>
#include <stdlib.h>
#include <string.h>

typedef void (*ArrayListFree)(void *);

typedef struct {
    void *data;
    size_t element_size;
    int length;
    int capacity;
    ArrayListFree free_func;
} ArrayList;

#define ALIST_INIT_CAPACITY 4

static inline void alist_init(ArrayList *arr, size_t element_size, ArrayListFree free_func) {
    arr->data = malloc(ALIST_INIT_CAPACITY * element_size);
    arr->element_size = element_size;
    arr->length = 0;
    arr->capacity = ALIST_INIT_CAPACITY;
    arr->free_func = free_func;
}

static inline void alist_init_with_cap(ArrayList *arr, size_t element_size, int capacity,
                                       ArrayListFree free_func) {
    arr->data = malloc(capacity * element_size);
    arr->element_size = element_size;
    arr->length = 0;
    arr->capacity = capacity;
    arr->free_func = free_func;
}

static inline void alist_init_from(ArrayList *arr, const void *data, int length,
                                   size_t element_size) {
    arr->data = malloc(length * element_size);
    memcpy(arr->data, data, length * element_size);
    arr->element_size = element_size;
    arr->length = length;
    arr->capacity = length;
    arr->free_func = NULL;
}

static inline void alist_grow(ArrayList *arr) {
    int new_capacity = arr->capacity == 0 ? ALIST_INIT_CAPACITY : arr->capacity * 2;
    void *new_data = realloc(arr->data, new_capacity * arr->element_size);
    if (new_data) {
        arr->data = new_data;
        arr->capacity = new_capacity;
    }
}

static inline void *alist_append(ArrayList *arr) {
    if (arr->length >= arr->capacity) {
        alist_grow(arr);
    }
    void *element = (char *)arr->data + (arr->length * arr->element_size);
    arr->length++;
    return element;
}

static inline void *alist_get(const ArrayList *arr, int index) {
    if (index < 0 || index >= arr->length)
        return NULL;
    return (char *)arr->data + (index * arr->element_size);
}

static inline void *array_list_get_ptr(const ArrayList *arr, int index) {
    if (index < 0 || index >= arr->length)
        return NULL;
    void **ptr = (void **)((char *)arr->data + (index * arr->element_size));
    return *ptr;
}

static inline void alist_set(ArrayList *arr, int index, const void *value) {
    if (index < 0 || index >= arr->length)
        return;
    memcpy((char *)arr->data + (index * arr->element_size), value, arr->element_size);
}

static inline void alist_insert(ArrayList *arr, int index, const void *value) {
    if (index < 0 || index > arr->length)
        return;
    if (arr->length >= arr->capacity) {
        alist_grow(arr);
    }
    char *dest = (char *)arr->data + ((index + 1) * arr->element_size);
    char *src = (char *)arr->data + (index * arr->element_size);
    memmove(dest, src, (arr->length - index) * arr->element_size);
    memcpy(src, value, arr->element_size);
    arr->length++;
}

static inline void alist_remove(ArrayList *arr, int index) {
    if (index < 0 || index >= arr->length)
        return;
    char *element = (char *)arr->data + (index * arr->element_size);
    if (arr->free_func) {
        arr->free_func(element);
    }
    char *dest = element;
    char *src = (char *)arr->data + ((index + 1) * arr->element_size);
    memmove(dest, src, (arr->length - index - 1) * arr->element_size);
    arr->length--;
}

static inline void alist_clear(ArrayList *arr) {
    if (arr->free_func && arr->data) {
        for (int i = 0; i < arr->length; i++) {
            void *element = (char *)arr->data + (i * arr->element_size);
            arr->free_func(element);
        }
    }
    arr->length = 0;
}

static inline void alist_destroy(ArrayList *arr) {
    if (arr->free_func && arr->data) {
        for (int i = 0; i < arr->length; i++) {
            void *element = (char *)arr->data + (i * arr->element_size);
            arr->free_func(element);
        }
    }
    free(arr->data);
    arr->data = NULL;
    arr->length = 0;
    arr->capacity = 0;
}

static inline int alist_length(const ArrayList *arr) {
    return arr->length;
}

static inline int alist_capacity(const ArrayList *arr) {
    return arr->capacity;
}

static inline void *alist_data(ArrayList *arr) {
    return arr->data;
}

static inline const void *alist_const_data(const ArrayList *arr) {
    return arr->data;
}

static inline bool alist_is_empty(const ArrayList *arr) {
    return arr->length == 0;
}

static inline void alist_truncate(ArrayList *arr, int new_length) {
    if (new_length < arr->length) {
        if (arr->free_func) {
            for (int i = new_length; i < arr->length; i++) {
                void *element = (char *)arr->data + (i * arr->element_size);
                arr->free_func(element);
            }
        }
        arr->length = new_length;
    } else if (new_length > arr->length && new_length <= arr->capacity) {
        int old_length = arr->length;
        arr->length = new_length;
        memset((char *)arr->data + (old_length * arr->element_size), 0,
               (new_length - old_length) * arr->element_size);
    }
}

static inline void alist_reverse(ArrayList *arr) {
    for (int i = 0; i < arr->length / 2; i++) {
        void *a = (char *)arr->data + (i * arr->element_size);
        void *b = (char *)arr->data + ((arr->length - 1 - i) * arr->element_size);
        char temp[256];
        memcpy(temp, a, arr->element_size);
        memcpy(a, b, arr->element_size);
        memcpy(b, temp, arr->element_size);
    }
}

static inline int alist_find(const ArrayList *arr, const void *value,
                             int (*cmp)(const void *, const void *)) {
    for (int i = 0; i < arr->length; i++) {
        void *element = (char *)arr->data + (i * arr->element_size);
        if (cmp(element, value) == 0) {
            return i;
        }
    }
    return -1;
}

#endif
