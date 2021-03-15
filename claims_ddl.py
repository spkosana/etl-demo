claims_table_stage = """
                            CREATE TABLE IF NOT EXISTS claimsdata_stage(
                            claim_id varchar,
                            paid_amount varchar,
                            date_of_service varchar,
                            days_supply int,
                            load_date DATETIME
                        );
                    
                    """

claims_table_main = """  
                                CREATE TABLE IF NOT EXISTS claimsdata(
                                    claim_id varchar(20) PRIMARY KEY,
                                    paid_amount varchar(10),
                                    date_of_service varchar(10),
                                    days_supply tinyint,
                                    load_date DATETIME
                                )

                        """
jobs_meta_create = """  
                            CREATE TABLE IF NOT EXISTS jobs_meta(
                                    load_id varchar PRIMARY KEY,
                                    data_filename varchar,
                                    spec_file varchar,
                                    spec_file_location varchar,
                                    etl_file varchar,
                                    source_location varchar,
                                    file_source_count varchar,
                                    processed_file_location varchar,
                                    stage_table_count varchar,
                                    target_count varchar,
                                    quality_check_result varchar,
                                    rejected_records_count varchar,
                                    reject_records_location varchar,
                                    record_created_date DATETIME
                                )

                        """

create_ddl_list = [claims_table_stage, claims_table_main, jobs_meta_create]
