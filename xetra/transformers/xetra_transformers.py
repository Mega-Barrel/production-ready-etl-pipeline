"""Xetra ETL Component"""
import logging
from datetime import datetime
from typing import NamedTuple

import pandas as pd

from xetra.common.s3 import S3BucketConnector
from xetra.common.meta_process import MetaProcess

class XetraSourceConfig(NamedTuple):
    """
    Class for source configuration data
    
    src_first_extract_date: determines the date for extracting the source
    src_columns: source column names
    src_col_date: column name for date in source
    src_col_isin: column name for isin in source
    src_col_time: column name for time in source
    src_col_start_price: column name for starging price in source
    src_col_min_price: column name for minimum price in source
    src_col_max_price: column name for maximum price in source
    src_col_traded_vol: column name for traded volumn in source
    """
    src_first_extract_date: str
    src_columns: list
    src_col_date: str
    src_col_isin: str
    src_col_time: str
    src_col_start_price: str
    src_col_min_price: str
    src_col_max_price: str
    src_col_traded_vol: str
    
class XetraTargetConfig(NamedTuple):
    """
    Class for target configuration data
    """
    trg_col_isin: str
    tar_col_date: str
    tar_col_op_price: str
    trg_col_clos_price: str
    trg_col_min_price: str
    trg_col_max_price: str
    trg_col_dail_price: str
    trg_col_ch_prev_clos: str
    trg_key: str
    trg_key_date_format: str
    trg_format: str
    
class XetraETL():
    """
    Reads the Xetra data, transforms and write the transformed to target
    """
    
    def __init__(
        self, 
        s3_bucket_src: S3BucketConnector,
        s3_bucket_trg: S3BucketConnector,
        meta_key: str,
        src_args: XetraSourceConfig,
        trg_args: XetraTargetConfig
    ):
        
        """
        Constructor for XetraTransformer
        
        :param s3_bucket_src: connection to source S3 bucket
        :param s3_bucket_trg: connection to target S3 bucket
        :param meta_key: used as self.meta_key -> key of meta file
        :param s3_bucket_src: NamedTuple class with source configuration data
        :param s3_bucket_src: NamedTuple class with target configuration data
        """
        self._logger = logging.getLogger(__name__)
        self.s3_bucket_source = s3_bucket_src
        self.s3_bucket_target = s3_bucket_trg
        self.meta_key = meta_key
        self.src_args = src_args
        self.trg_args = trg_args
        self.extract_date, self.extract_date_list = MetaProcess.return_date_list(
            self.src_args.src_first_extract_date,
            self.meta_key,
            self.s3_bucket_target
        )
        self.meta_update_list = None

    def extract(self):
        """
        Read the source data and concatenates them to one Pandas DataFrame
        
        :returns:
            data_frame: Pandas DataFrame with the extracted data
        """
        self._logger.info('Extracting Xetra source files started...')
        files = [
            key for date in self.extract_date_list\
                for key in self.s3_bucket_source.list_files_in_prefix(date)
        ]
        if not files:
            data_frame = pd.DataFrame()
        else:
            data_frame = pd.concat(
                [
                    self.s3_bucket_source.read_csv_to_df(file) for file in files
                ],
                ignore_index=True
            )
        self._logger.info('Extracting Xetra source files finished.')
        return data_frame

    def transform_report1(self, data_frame: pd.dataFrame):
        """
        Applies the necessary transformation to create report 1
        
        :param data_frame: Pandas DataFrame as Input
        
        :returns:
            data_frame: Transformed Pandas DataFrame as Output
        """
        if data_frame.empty:
            self._logger.info('The dataframe is empty. No transformations will be applied.')
            return data_frame
        self._logger.info('Applying transformations to Xetra source data for report 1 started...')
        # Filtering necessary source columns
        data_frame = data_frame.loc[:, self.src_args.src_columns]
        # Removing rows with missing values
        data_frame.dropna(inplace=True)
        # Calculating opening price per ISIN and day
        
