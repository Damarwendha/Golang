# Golang
### Numerical Type Ranges
![image](https://github.com/Damarwendha/Golang/assets/143293717/5a04f7c7-4f6a-458d-86e9-126c6fd33fed)
<br/>
Always use type that fit your needs, as this will optimize memory usage and lead to better performance

### Golang Notes
- WaitGroup purpose is to wait until goroutines has finished executing before the program to end

- Mutex purpose is to ensure only one goroutine accesses data at a time. Basically, is to prevent changing data at the same time 

- Channel should be closed if it's not being used, because it will cause memory leak

# PostgreSQL
```SELECT column_name, COUNT(*) FROM table_name WHERE condition GROUP BY column_name;```
<br />
Example: ```SELECT major, COUNT(*) AS jumlah_peserta FROM mst_student WHERE major LIKE 'S%' GROUP BY major ;```
<br />
Output:
<br />
![image](https://github.com/Damarwendha/Golang/assets/143293717/ec61a1f9-80f4-4854-bc31-1f9892d7d981)

### Refer/Connect table to another table
```ALTER TABLE table_name ADD CONSTRAINT constraint_name FOREIGN KEY(column_name) REFERENCES table_name_sec(column_name_sec)```
<br />
Example: ```ALTER TABLE mst_product ADD CONSTRAINT fk_product_store FOREIGN KEY(store_id) REFERENCES mst_store(id)```

### Using too much Join query will lead to bad query performance

# USEFUL VIDEO'S
-) Learn Database Normalization
<br />
https://youtu.be/GFQaEYEc8_8?si=-3F8fcKEYroFU9YE
<br />
https://youtu.be/UC_tJx4MBgk?si=Aa9aXHRZWpGfrgvZ
