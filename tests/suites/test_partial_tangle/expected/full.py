# What must not be in the target database: catalog rows of the source that the restore must leave out.
# A row may be {'row': (...), 'min_pg': N, 'max_pg': N}: it counts only on servers with server_version_num in that range.
{}
