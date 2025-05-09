-- table name im_attributes
id,bigint unsigned
name,varchar(150)
type,"enum('TEXT','NUMBER','DATE')"
code,varchar(150)
mandatory_for_all_products,tinyint(1)
flat_table_index,int
is_system_defined,tinyint(1)
created_by,int
updated_by,int
deleted_at,timestamp
created_at,timestamp
updated_at,timestamp