"""
Methods for processing meta files
"""

import pandas as pd
from xetra.common.s3 import S3BucketConnector


class MetaProcess():
    """
    class for working with meta files
    """

    @staticmethod
    def update_meta_file(extract_date_list: list, meta_key: str, s3_bucket_meta: S3BucketConnector):
        """
        Updating the meta file with the processed Xetra dates and todays date as processed data
        
        :param: extract_date_list -> a list of dates that are extracted from the source
        :param: meta_key -> key of meta file on the S3 bucket
        :param: s3_bucket_meta -> S3BucketConnector for the bucket with the meta file
        """
        # Creating empty DataFrame using the meta file column names
        df_new = pd.DataFrame(
            columns = [
                MetaProcessFormat.META_SOURCE_DATE_COL.value,
                MetaProcessFormat.META_PROCESS_COL.value
            ]
        )
        # Filling the date column with extract_date_list
    
    @staticmethod
    def return_date_list():
        pass