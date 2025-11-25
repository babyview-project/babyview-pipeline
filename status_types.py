class VideoStatus:
    TO_BE_DELETED = "to_be_deleted"
    TO_BE_REPROCESS = "to_be_reprocessed"

    REMOVED = "successfully_deleted_from_GCP"
    PROCESSED = "successfully_processed"

    META_FAIL = "error_in_meta_extraction"
    REMOVE_FAIL = "error_in_GCP_deletion"
    COMPRESS_FAIL = "error_in_compression"
    ROTATE_FAIL = "error_in_rotation"
    BLACKOUT_FAIL = "error_in_blackout"

    NOT_FOUND = "not_found"