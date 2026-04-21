int commit_create(const char *message, ObjectID *commit_id_out) {
    Commit commit;

    /* Step 1: Create tree object from current index */
    if (tree_from_index(&commit.tree) != 0)
        return -1;

    /* Step 2: Try reading parent commit from HEAD */
    if (head_read(&commit.parent) == 0)
        commit.has_parent = 1;
    else
        commit.has_parent = 0;

    /* Step 3: Set author */
    snprintf(commit.author,
             sizeof(commit.author),
             "%s",
             pes_author());

    /* Step 4: Current timestamp */
    commit.timestamp = (uint64_t)time(NULL);

    /* Step 5: Set commit message */
    snprintf(commit.message,
             sizeof(commit.message),
             "%s",
             message);

    /* Step 6: Serialize commit */
    void *commit_data = NULL;
    size_t commit_len = 0;

    if (commit_serialize(&commit,
                         &commit_data,
                         &commit_len) != 0)
        return -1;

    /* Step 7: Store commit object */
    if (object_write(OBJ_COMMIT,
                     commit_data,
                     commit_len,
                     commit_id_out) != 0) {
        free(commit_data);
        return -1;
    }

    free(commit_data);

    /* Step 8: Move HEAD to new commit */
    if (head_update(commit_id_out) != 0)
        return -1;

    return 0;
}
