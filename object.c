int object_write(ObjectType type, const void *data, size_t len, ObjectID *id_out) {
    char type_str[10];
    char header[64];

    /* Convert enum type to string */
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

    /* Combine header + actual data */
    size_t total_len = header_len + len;
    char *full_object = malloc(total_len);
    if (!full_object)
        return -1;

    memcpy(full_object, header, header_len);
    memcpy(full_object + header_len, data, len);

    /* Compute SHA-256 hash of full object */
    compute_hash(full_object, total_len, id_out);

    /* Deduplication: if object already exists, return success */
    if (object_exists(id_out)) {
        free(full_object);
        return 0;
    }

    /* Get final object path */
    char final_path[512];
    object_path(id_out, final_path, sizeof(final_path));

    /* Create shard directory (.pes/objects/XX/) */
    char dir_path[512];
    strncpy(dir_path, final_path, sizeof(dir_path));

    char *last_slash = strrchr(dir_path, '/');
    if (!last_slash) {
        free(full_object);
        return -1;
    }

    *last_slash = '\0';
    mkdir(dir_path, 0755);

    /* Create temporary file path */
    char temp_path[512];
    snprintf(temp_path, sizeof(temp_path), "%s/temp_object", dir_path);

    /* Open temp file */
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

    /* Flush file to disk */
    fsync(fd);
    close(fd);

    /* Atomically rename temp file to final object path */
    if (rename(temp_path, final_path) != 0) {
        free(full_object);
        return -1;
    }

    /* fsync the shard directory to persist rename */
    int dir_fd = open(dir_path, O_RDONLY);
    if (dir_fd >= 0) {
        fsync(dir_fd);
        close(dir_fd);
    }

    free(full_object);
    return 0;
}

int object_read(const ObjectID *id, ObjectType *type_out, void **data_out, size_t *len_out) {
    char path[512];
    object_path(id, path, sizeof(path));

    /* Open file */
    FILE *fp = fopen(path, "rb");
    if (!fp)
        return -1;

    /* Find file size */
    fseek(fp, 0, SEEK_END);
    long file_size = ftell(fp);
    rewind(fp);

    if (file_size <= 0) {
        fclose(fp);
        return -1;
    }

    /* Read entire file */
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

    /* Verify integrity by recomputing hash */
    ObjectID computed_id;
    compute_hash(buffer, file_size, &computed_id);

    if (memcmp(computed_id.hash, id->hash, HASH_SIZE) != 0) {
        free(buffer);
        return -1;
    }

    /* Find null terminator separating header and data */
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

    /* Set object type */
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

    /* Allocate and copy actual data */
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
