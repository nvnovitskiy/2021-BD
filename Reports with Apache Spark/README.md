# L2 - Reports with Apache Spark

____

## Задание на лабораторную работу

+ Сформировать отчёт с информацией о 10 наиболее популярных языках программирования по итогам года за период с 2010 по 2019 года.


```python
def language_detection(x):
  """
  Данная функция переводит весь текст в нижний регистр
  и ищет название языка программирования в каждой строке,
  если язык был найден, то создается кортеж, иначе None
  """
  tag = None
  for language in languages_list:
    if "<" + language.lower() + ">" in x._Tags.lower():
      tag = language
      break
  if tag is None:
    return None
  return (x._Id, tag)


def check_date(x, year):
  """
  Данная функция была написана для фильтрации по датам,
  так как нас интересует период с 2010 год по 2019 год
  """
  start = datetime(year=year, month=1, day=1)
  end = datetime(year=year, month=12, day=31)
  CreationDate = x._CreationDate
  return CreationDate >= start and CreationDate <= end


"""
Данный кусок кода сначала убирает пустые значения и оставляет диапазон с 2010 по 2019 год,
далее мы находим язык программирования в каждой строке и убираем пустые значения, если не был 
найден, потом смотрим, сколько раз упоминался каждый язык программирования в каждом годе и сортируем по 
количеству повторений и в конце идет сортировка от большего к меньшему по количеству упоминаний.
"""

final_result = {}
for year in range(2010, 2020):
  final_result[year] = posts_sample.rdd\
      .filter(lambda x: x._Tags is not None and check_date(x, year))\
      .map(language_detection)\
      .filter(lambda x: x is not None)\
      .keyBy(lambda x: x[1])\
      .aggregateByKey(
          0,
          lambda x, y: x + 1,
          lambda x1, x2: x1 + x2,
      )\
      .sortBy(lambda x: x[1], ascending=False)\
      .toDF()
  final_result[year] = final_result[year].select(col("_1").alias("Programming_language"), 
                                                 col("_2").alias(f"Number_of_mentions_in_{year}")).limit(10)
  final_result[year].show()

+--------------------+--------------------------+
|Programming_language|Number_of_mentions_in_2010|
+--------------------+--------------------------+
|                Java|                        52|
|          JavaScript|                        44|
|                 PHP|                        42|
|              Python|                        25|
|         Objective-C|                        22|
|                   C|                        20|
|                Ruby|                        11|
|              Delphi|                         7|
|                   R|                         3|
|                Bash|                         3|
+--------------------+--------------------------+

+--------------------+--------------------------+
|Programming_language|Number_of_mentions_in_2011|
+--------------------+--------------------------+
|                 PHP|                        97|
|                Java|                        92|
|          JavaScript|                        82|
|              Python|                        35|
|         Objective-C|                        33|
|                   C|                        24|
|                Ruby|                        17|
|              Delphi|                         8|
|                Perl|                         8|
|                Bash|                         7|
+--------------------+--------------------------+

+--------------------+--------------------------+
|Programming_language|Number_of_mentions_in_2012|
+--------------------+--------------------------+
|                 PHP|                       136|
|          JavaScript|                       129|
|                Java|                       124|
|              Python|                        65|
|         Objective-C|                        45|
|                   C|                        27|
|                Ruby|                        25|
|                Bash|                         9|
|                   R|                         9|
|              MATLAB|                         6|
+--------------------+--------------------------+

+--------------------+--------------------------+
|Programming_language|Number_of_mentions_in_2013|
+--------------------+--------------------------+
|          JavaScript|                       196|
|                Java|                       191|
|                 PHP|                       173|
|              Python|                        87|
|         Objective-C|                        40|
|                   C|                        36|
|                Ruby|                        30|
|                   R|                        25|
|                Bash|                        11|
|               Scala|                        10|
+--------------------+--------------------------+

+--------------------+--------------------------+
|Programming_language|Number_of_mentions_in_2014|
+--------------------+--------------------------+
|          JavaScript|                       235|
|                Java|                       228|
|                 PHP|                       154|
|              Python|                       103|
|                   C|                        52|
|         Objective-C|                        49|
|                   R|                        28|
|                Ruby|                        20|
|              MATLAB|                        16|
|                Bash|                        13|
+--------------------+--------------------------+

+--------------------+--------------------------+
|Programming_language|Number_of_mentions_in_2015|
+--------------------+--------------------------+
|          JavaScript|                       270|
|                Java|                       208|
|                 PHP|                       147|
|              Python|                       119|
|                   R|                        43|
|                   C|                        38|
|         Objective-C|                        30|
|                Ruby|                        20|
|              MATLAB|                        16|
|               Scala|                        13|
+--------------------+--------------------------+

+--------------------+--------------------------+
|Programming_language|Number_of_mentions_in_2016|
+--------------------+--------------------------+
|          JavaScript|                       271|
|                Java|                       178|
|              Python|                       140|
|                 PHP|                       126|
|                   R|                        50|
|                   C|                        32|
|                Ruby|                        21|
|                Bash|                        16|
|               Scala|                        16|
|              MATLAB|                        15|
+--------------------+--------------------------+

+--------------------+--------------------------+
|Programming_language|Number_of_mentions_in_2017|
+--------------------+--------------------------+
|          JavaScript|                       244|
|                Java|                       204|
|              Python|                       185|
|                 PHP|                       122|
|                   R|                        53|
|                   C|                        24|
|         Objective-C|                        19|
|                Ruby|                        16|
|          TypeScript|                        14|
|          PowerShell|                        14|
+--------------------+--------------------------+

+--------------------+--------------------------+
|Programming_language|Number_of_mentions_in_2018|
+--------------------+--------------------------+
|              Python|                       214|
|          JavaScript|                       194|
|                Java|                       145|
|                 PHP|                        99|
|                   R|                        63|
|                   C|                        24|
|               Scala|                        22|
|          TypeScript|                        21|
|          PowerShell|                        13|
|                Bash|                        12|
+--------------------+--------------------------+

+--------------------+--------------------------+
|Programming_language|Number_of_mentions_in_2019|
+--------------------+--------------------------+
|              Python|                       162|
|          JavaScript|                       131|
|                Java|                        95|
|                 PHP|                        59|
|                   R|                        36|
|                   C|                        14|
|                  Go|                         9|
|              MATLAB|                         9|
|                Dart|                         9|
|                Bash|                         8|
+--------------------+--------------------------+

# Запись результатов в файлы
for year in final_result.keys():
    final_result[year].write.format("parquet").save(f"/content/final_result/top_{year}")
```
