// index.c — Staging area implementation
//
// Text format of .pes/index (one entry per line, sorted by path):
//
//   <mode-octal> <64-char-hex-hash> <mtime-seconds> <size> <path>
//
// Example:
//   100644 a1b2c3d4e5f6... 1699900000 42 README.md
//
// PROVIDED functions: index_find, index_remove, index_status
// TODO functions:     index_load, index_save, index_add

#include "index.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <dirent.h>

/* Forward declaration from object.c */
int object_write(ObjectType type, const void *data, size_t len, ObjectID *id_out);

/* ─── PROVIDED ───────────────────────────────────────────── */

/* Find index entry by path */
IndexEntry* index_find(Index *index, const char *path) {
    for (int i = 0; i < index->count; i++) {
        if (strcmp(index->entries[i].path, path) == 0)
            return &index->entries[i];
    }
    return NULL;
}

/* Remove file from index */
int index_remove(Index *index, const char *path) {
    for (int i = 0; i < index->count; i++) {
        if (strcmp(index->entries[i].path, path) == 0) {
            int remaining = index->count - i - 1;

            if (remaining > 0) {
                memmove(&index->entries[i],
                        &index->entries[i + 1],
                        remaining * sizeof(IndexEntry));
            }

            index->count--;
            return index_save(index);
        }
    }

    fprintf(stderr,
            "error: '%s' is not in the index\n",
            path);

    return -1;
}

/* Status display */
int index_status(const Index *index) {
    printf("Staged changes:\n");

    if (index->count == 0) {
        printf("  (nothing to show)\n");
    } else {
        for (int i = 0; i < index->count; i++) {
            printf("  staged: %s\n",
                   index->entries[i].path);
        }
    }

    printf("\n");
    return 0;
}

/* ─── TODO IMPLEMENTED ───────────────────────────────────── */

/* Load index from .pes/index */
int index_load(Index *index) {
    FILE *fp = fopen(INDEX_FILE, "r");

    index->count = 0;

    /* No index file yet = empty index */
    if (!fp)
        return 0;

    while (index->count < MAX_INDEX_ENTRIES) {
        IndexEntry *entry = &index->entries[index->count];
        char hash_hex[HASH_HEX_SIZE + 1];

        int result = fscanf(fp,
                            "%o %64s %ld %zu %255s",
                            &entry->mode,
                            hash_hex,
                            &entry->mtime_sec,
                            &entry->size,
                            entry->path);

        if (result == EOF)
            break;

        if (result != 5) {
            fclose(fp);
            return -1;
        }

        if (hex_to_hash(hash_hex, &entry->id) != 0) {
            fclose(fp);
            return -1;
        }

        index->count++;
    }

    fclose(fp);
    return 0;
}

/* Save index atomically */
int index_save(const Index *index) {
    char temp_path[512];

    snprintf(temp_path,
             sizeof(temp_path),
             "%s.tmp",
             INDEX_FILE);

    FILE *fp = fopen(temp_path, "w");
    if (!fp)
        return -1;

    for (int i = 0; i < index->count; i++) {
        char hash_hex[HASH_HEX_SIZE + 1];

        hash_to_hex(&index->entries[i].id,
                    hash_hex);

        fprintf(fp,
                "%o %s %ld %zu %s\n",
                index->entries[i].mode,
                hash_hex,
                index->entries[i].mtime_sec,
                index->entries[i].size,
                index->entries[i].path);
    }

    fflush(fp);
    fsync(fileno(fp));
    fclose(fp);

    if (rename(temp_path, INDEX_FILE) != 0)
        return -1;

    return 0;
}

/* Stage file into index */
int index_add(Index *index, const char *path) {
    FILE *fp = fopen(path, "rb");

    if (!fp) {
        fprintf(stderr,
                "error: cannot open file '%s'\n",
                path);
        return -1;
    }

    /* Get file size */
    fseek(fp, 0, SEEK_END);
    long file_size = ftell(fp);
    rewind(fp);

    if (file_size < 0) {
        fclose(fp);
        return -1;
    }

    /* Read file contents */
    void *buffer = malloc(file_size);

    if (!buffer) {
        fclose(fp);
        return -1;
    }

    if (fread(buffer,
              1,
              file_size,
              fp) != (size_t)file_size) {
        free(buffer);
        fclose(fp);
        return -1;
    }

    fclose(fp);

    /* Store blob object */
    ObjectID blob_id;

    if (object_write(OBJ_BLOB,
                     buffer,
                     file_size,
                     &blob_id) != 0) {
        free(buffer);
        return -1;
    }

    free(buffer);

    /* Get metadata */
    struct stat st;

    if (stat(path, &st) != 0)
        return -1;

    /* Update existing or add new */
    IndexEntry *existing = index_find(index, path);

    if (existing) {
        existing->id = blob_id;
        existing->mode = st.st_mode;
        existing->mtime_sec = st.st_mtime;
        existing->size = st.st_size;
    } else {
        if (index->count >= MAX_INDEX_ENTRIES)
            return -1;

        IndexEntry *entry = &index->entries[index->count];

        strcpy(entry->path, path);
        entry->id = blob_id;
        entry->mode = st.st_mode;
        entry->mtime_sec = st.st_mtime;
        entry->size = st.st_size;

        index->count++;
    }

    return index_save(index);
}
