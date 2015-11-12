# prepare

Easy preparation of data set for data analysis or machine learning tasks!

### Usage

Sample from example.csv file
```
First, nice, name, another, addition, cpu
1,"A","App", 0.41, 0.85, "N"
15, B, 10, 777, 4
8, "B", 0.74, 0.9665, 10
NA, "A", 4, 0.74
4, "A", 07, 0.111, 5, "This is fun"
1, "A", 0.88. NaN, 2
```

```python
import prepare
item = prepare.Prepare()
print(item.read('example1.csv').preprocess(replace_na='mean', replace_string_na=' ').toDF())
```


### API

#### applyColumnEvent
Set event to coulmn

```python
item.read('example1.csv').applyColumnEvent('column_name', lambda x: 'A').toDF()
```


```applyRowEvent``` - Set event to row
