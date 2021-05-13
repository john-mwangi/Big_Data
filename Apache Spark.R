knitr::opts_chunk$set(echo = TRUE)

library(DBI)
library(sparklyr)
library(tidyverse)

spark_available_versions()

# spark_install()

spark_installed_versions()

sc <- spark_connect(master = "local")

cars <- copy_to(dest = sc, df = mtcars)

cars

dbListTables(conn = sc)

cars %>% 
  select(mpg, hp) %>% 
  ggplot(aes(x = hp, y = mpg))+
  geom_point()+
  geom_smooth()+
  labs(title = "Miles/gallon vs horse-power")

lm_model <- ml_linear_regression(x = cars, formula = mpg ~ hp)

summary(lm_model)

lm_model %>% 
  ml_predict(dataset = copy_to(dest = sc, 
                               df = data.frame(hp = 250 + 10*1:10, 
                                               source = "prediction"))) %>% 
  select(hp, mpg = prediction, source) %>% 
  full_join(cars %>% 
              mutate(source = "actual") %>% 
              select(mpg, hp, source)) %>% 
  ggplot(aes(x = hp, y = mpg, colour = source))+
  geom_point()+
  labs(title = "mpg prediction using hp",
       colour = "Origin")

library(sparklyr.nested)

cars %>% 
  sdf_nest(hp) %>% 
  group_by(cyl) %>% 
  summarise(data = collect_list(data))

mtcars %>%
  select(cyl,hp) %>% 
  nest(hp)

cars %>% 
  spark_apply(f = function(x){round(x)})

cars %>% 
  spark_apply(~round(.))

cars %>% 
  collect() %>% 
  map_df(.f = round)

spark_read_csv(sc = sc, 
               name = "mem_9", 
               path = "./Mem9_2.csv", 
               overwrite = TRUE,
               temporary = FALSE)

tbl(src = sc, "mem_9")

class(mtcars)

copy_to(dest = sc, df = mtcars, name = "mtcars", overwrite = TRUE)

arrow::write_parquet(x = mtcars, sink = "./cars.parquet")

spark_read_parquet(sc = sc, name = "cars_parq", path = "./cars.parquet")

dbListTables(conn = sc)

dir.create("stream_in")
dir.create("stream_out")

write_csv(x = mtcars, file = "./stream_in/cars_1.csv")

stream <- stream_read_csv(sc = sc, path = "./stream_in/") %>% 
  select(mpg, cyl, disp) %>% 
  stream_write_csv(path = "./stream_out/")

sc <- spark_connect(master = "local")

spark_read_csv(sc = sc, 
               name = "mem", 
               path = "./mem_1000.csv", 
               overwrite = TRUE)

dbListTables(conn = sc)

spark_disconnect(sc = sc)

sc <- spark_connect(master = "local")

dbListTables(conn = sc)

# Requires HDFS
spark_write_parquet(x = mem, 
                    path = "file:///E:/public-projects/Big_Data/data/mem_s.parquet")

dbListTables(conn = sc)

tbl(src = sc, "mem") %>% 
  collect() %>% 
  arrow::write_parquet(sink = "./data/mem_a.parquet")

# Test
arrow::write_parquet(x = mem %>% 
                       collect(), 
                     sink = "./data/mem_a.parquet")

write_csv(x = mem %>% 
            collect(),
          file = "./data/mem.csv")

spark_disconnect_all()

rm(list = ls())
