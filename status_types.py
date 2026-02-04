class VideoStatus:
    TO_BE_DELETED = "to_be_deleted"
    TO_BE_REPROCESS = "to_be_reprocessed"

    REMOVED = "successfully_deleted_from_GCP"
    PROCESSED = "successfully_processed"

    DOWNLOAD_FAIL = "error_in_download"
    META_FAIL = "error_in_meta_extraction"
    ZIP_FAIL = "error_in_zip"
    IMU_FAIL = "error_in_imu"
    REMOVE_FAIL = "error_in_GCP_deletion"
    COMPRESS_FAIL = "error_in_compression"
    ROTATE_FAIL = "error_in_rotation"
    BLACKOUT_FAIL = "error_in_blackout"
    UPLOAD_RAW_FAIL = "error_in_upload_raw"
    UPLOAD_ZIP_FAIL = "error_in_upload_zip"
    UPLOAD_COMPRESS_FAIL = "error_in_upload_compress"
    UNEXPECTED_FAIL = "error_unexpected"

    NOT_FOUND = "not_found"