// object.c — Content-addressable object store
//
// Every piece of data (file contents, directory listings, commits) is stored
// as an "object" named by its SHA-256 hash. Objects are stored under
// .pes/objects/XX/YYYYYY... where XX is the first two hex characters of the
// hash (directory sharding).
//
// PROVIDED functions: compute_hash, object_path, object_exists, hash_to_hex, hex_to_hash
// TODO functions:     object_write, object_read

#include "pes.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <openssl/evp.h>

// ─── PROVIDED ────────────────────────────────────────────────────────────────

void hash_to_hex(const ObjectID *id, char *hex_out) {
    for (int i = 0; i < HASH_SIZE; i++) {
        sprintf(hex_out + i * 2, "%02x", id->hash[i]);
    }
    hex_out[HASH_HEX_SIZE] = '\0';
}

int hex_to_hash(const char *hex, ObjectID *id_out) {
    if (strlen(hex) < HASH_HEX_SIZE) return -1;
    for (int i = 0; i < HASH_SIZE; i++) {
        unsigned int byte;
        if (sscanf(hex + i * 2, "%2x", &byte) != 1) return -1;
        id_out->hash[i] = (uint8_t)byte;
    }
    return 0;
}

void compute_hash(const void *data, size_t len, ObjectID *id_out) {
    unsigned int hash_len;
    EVP_MD_CTX *ctx = EVP_MD_CTX_new();
    EVP_DigestInit_ex(ctx, EVP_sha256(), NULL);
    EVP_DigestUpdate(ctx, data, len);
    EVP_DigestFinal_ex(ctx, id_out->hash, &hash_len);
    EVP_MD_CTX_free(ctx);
}

// Get the filesystem path where an object should be stored.
// Format: .pes/objects/XX/YYYYYYYY...
// The first 2 hex chars form the shard directory; the rest is the filename.
void object_path(const ObjectID *id, char *path_out, size_t path_size) {
    char hex[HASH_HEX_SIZE + 1];
    hash_to_hex(id, hex);
    snprintf(path_out, path_size, "%s/%.2s/%s", OBJECTS_DIR, hex, hex + 2);
}

int object_exists(const ObjectID *id) {
    char path[512];
    object_path(id, path, sizeof(path));
    return access(path, F_OK) == 0;
}

// ─── TODO: Implement these ──────────────────────────────────────────────────

// Write an object to the store.
int object_write(ObjectType type, const void *data, size_t len, ObjectID *id_out) {
    char type_str[10];
    char header[64];

    if (type == OBJ_BLOB)
        strcpy(type_str, "blob");
    else if (type == OBJ_TREE)
        strcpy(type_str, "tree");
    else if (type == OBJ_COMMIT)
        strcpy(type_str, "commit");
    else
        return -1;

    /* Create header: "<type> <size>\0" */
    int header_len = snprintf(header, sizeof(header), "%s %zu", type_str, len) + 1;

    /* Combine header + data */
    size_t total_len = header_len + len;
    char *full_object = malloc(total_len);
    if (!full_object)
        return -1;

    memcpy(full_object, header, header_len);
    memcpy(full_object + header_len, data, len);

    /* Compute hash */
    compute_hash(full_object, total_len, id_out);

    /* Deduplication */
    if (object_exists(id_out)) {
        free(full_object);
        return 0;
    }

    /* Final path */
    char final_path[512];
    object_path(id_out, final_path, sizeof(final_path));

    /* Directory path */
    char dir_path[512];
    strncpy(dir_path, final_path, sizeof(dir_path) - 1);
    dir_path[sizeof(dir_path) - 1] = '\0';

    char *last_slash = strrchr(dir_path, '/');
    if (!last_slash) {
        free(full_object);
        return -1;
    }

    *last_slash = '\0';

    /* Create shard directory */
    mkdir(dir_path, 0755);

    /* Temp file path */
    char temp_path[512];
    snprintf(temp_path, sizeof(temp_path), "%.*s/temp_object",
         (int)(sizeof(temp_path) - 13), dir_path);

    int fd = open(temp_path, O_CREAT | O_WRONLY | O_TRUNC, 0644);
    if (fd < 0) {
        free(full_object);
        return -1;
    }

    /* Write full object */
    if (write(fd, full_object, total_len) != (ssize_t)total_len) {
        close(fd);
        free(full_object);
        return -1;
    }

    fsync(fd);
    close(fd);

    /* Atomic rename */
    if (rename(temp_path, final_path) != 0) {
        free(full_object);
        return -1;
    }

    /* fsync directory */
    int dir_fd = open(dir_path, O_RDONLY);
    if (dir_fd >= 0) {
        fsync(dir_fd);
        close(dir_fd);
    }

    free(full_object);
    return 0;
}

// Read an object from the store.
int object_read(const ObjectID *id, ObjectType *type_out, void **data_out, size_t *len_out) {
    char path[512];
    object_path(id, path, sizeof(path));

    FILE *fp = fopen(path, "rb");
    if (!fp)
        return -1;

    fseek(fp, 0, SEEK_END);
    long file_size = ftell(fp);
    rewind(fp);

    if (file_size <= 0) {
        fclose(fp);
        return -1;
    }

    char *buffer = malloc(file_size);
    if (!buffer) {
        fclose(fp);
        return -1;
    }

    if (fread(buffer, 1, file_size, fp) != (size_t)file_size) {
        free(buffer);
        fclose(fp);
        return -1;
    }

    fclose(fp);

    /* Verify integrity */
    ObjectID computed_id;
    compute_hash(buffer, file_size, &computed_id);

    if (memcmp(computed_id.hash, id->hash, HASH_SIZE) != 0) {
        free(buffer);
        return -1;
    }

    /* Find null separator */
    char *null_pos = memchr(buffer, '\0', file_size);
    if (!null_pos) {
        free(buffer);
        return -1;
    }

    size_t header_len = null_pos - buffer + 1;

    /* Parse header */
    char type_str[10];
    size_t size;

    if (sscanf(buffer, "%9s %zu", type_str, &size) != 2) {
        free(buffer);
        return -1;
    }

    if (strcmp(type_str, "blob") == 0)
        *type_out = OBJ_BLOB;
    else if (strcmp(type_str, "tree") == 0)
        *type_out = OBJ_TREE;
    else if (strcmp(type_str, "commit") == 0)
        *type_out = OBJ_COMMIT;
    else {
        free(buffer);
        return -1;
    }

    if (header_len + size > (size_t)file_size) {
        free(buffer);
        return -1;
    }

    *data_out = malloc(size);
    if (!*data_out) {
        free(buffer);
        return -1;
    }

    memcpy(*data_out, buffer + header_len, size);
    *len_out = size;

    free(buffer);
    return 0;
}
