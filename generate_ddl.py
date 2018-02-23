import sys
import io
import argparse
from messytables import CSVTableSet, type_guess, \
  types_processor, headers_guess, headers_processor, \
  offset_processor, any_tableset
from messytables import (DateType, StringType,
                         DecimalType, IntegerType,
                         DateUtilType, BoolType)
import smart_open
import argparse
 
parser = argparse.ArgumentParser(description='create ddl template for Hive with column type guessing.')
parser.add_argument('s3path', action='store', metavar='s3path', type=str, help='s3 file path')
parser.add_argument('--delimiter', action='store', nargs='?', metavar='delimiter', type=str, default=',', help='file delimiter')
parser.add_argument('--date-format', action='store', nargs='?', metavar='format', type=str, default='%Y/%m/%d', help='date column format')
args = parser.parse_args()
 
s3path = args.s3path
delimiter = args.delimiter.strip("\"").strip("\'")
date_format = args.date_format
folder,filename = s3path.rsplit('/', 1)
print("file path:", s3path)
print("delimiter:",delimiter)
print("date_format:",date_format)
 
# getting data
generator = (i for i in smart_open.smart_open(s3path))
head_lst = list(next(generator) for _ in range(100))
head_str = map(lambda x: str(x).strip("b").strip("\"").strip('\\n').strip("\'"), head_lst)
head_join = "\n".join(head_str)
f = io.BytesIO(bytes(head_join, 'utf-8'))
 
# gussing
row_set = CSVTableSet(f, delimiter=delimiter).tables[0]
offset, headers = headers_guess(row_set.sample)
row_set.register_processor(headers_processor(headers))
row_set.register_processor(offset_processor(offset + 1))
types = type_guess(row_set.sample)
#types = type_guess(row_set.sample, strict=True)
print('guessed types:',types)
 
# constructing ddl
cols = []
for indx, typ in enumerate(types):
  if typ == StringType():
    cols.append("  `a%s` string" % (indx))
  elif typ == DateType(date_format):
    cols.append("  `a%s` date" % (indx))
  elif typ == DecimalType():
    cols.append("  `a%s` double" % (indx))
  elif typ == IntegerType():
    cols.append("  `a%s` int" % (indx))
  elif typ == BoolType():
    cols.append("  `a%s` boolean" % (indx))
  else:
    raise Exception("A type of column %indx cannot be handled. %s " % (indx, typ))
cols_str = ",\n".join(cols)
 
ddl = '''----------------------------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS default.%s (
%s
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    "separatorChar" = "%s",
    "quoteChar"     = "'",
    "escapeChar"    = "\\\\"
) LOCATION '%s'
TBLPROPERTIES ('has_encrypted_data'='false');
----------------------------------------------------------''' % (filename.rsplit('.', 1)[0], cols_str, delimiter, folder)
print(ddl)
