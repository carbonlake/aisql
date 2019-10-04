## AISQL:  A SparkSQL extension for AI application

使用方式:
```scala
val session = SparkSession.builder()
    .master("local")
    .withExtensions(new AISQLExtension)
    .getOrCreate()
session.sql("...")
```

语法:
- 非结构化数据表格

```sql
-- 创建图片表格
CREATE TABLE image_table(
  path STRING,
  modificationTime TIMESTAMP,
  length LONG,
  image BINARY
) AS CARBONDATA;

-- 图片入库
INSERT INTO image_table 
SELECT * FROM binaryfile.`path/to/image`;
```


- 使用Python UDF

```sql
REGISTER PYFUNC foo INPATH 'my_path' AS my_udf;
SELECT my_udf(image) FROM table1;
```

- 使用内置的图片变换UDF

AISQL内置了20+种常用图片变换UDF，例如图片裁剪、旋转、加噪声、亮度增强等。
```sql
-- 调整图片大小为256*256
INSERT INTO result_table
SELECT RESIZE(image, 256, 256) FROM image_table;
```


- 运行任意Python脚本

```sql
RUN SCRIPT 'path/to/my_script.py' 
PYFUNC foo 
WITH PARAMS ('x'='4')
OUTPUT (value INT);
```


- 使用本地TensorFlow模型做推理

```sql
-- 将本地的TF模型注册为UDF
REGISTER LOCAL MODEL INPATH 'path/to/model' AS my_udf 
OPTIONS ('outut_operation'='resnet_v1_50/SpatialSqueeze:0', ...');

-- 使用UDF做批量推理
SELECT my_udf(image) from image_table;
```


- 使用华为云OCR API

```sql
-- 调用OCR API识别图片
SELECT ocr_id_card(image) 
from image_table;
```


- 使用华为云ModelArts服务做模型训练和推理

```sql
-- 创建训练模板
CREATE EXPERIMENT flower_exp
OPTIONS('dataset_name'='flower', ...);

-- 调用ModelArts训练模型
CREATE MODEL model1 
USING EXPERIMENT flower_exp
OPTIONS('train_url'='path/to/flower_model/');

-- 将模型注册为UDF
REGISTER MODEL flower_exp.model1 AS flower_udf;

-- 使用UDF做批量推理
SELECT flower_udf(image) FROM image_table;

-- 显示模型信息
SELECT * FROM MODEL_INFO(flower_udf);
```

