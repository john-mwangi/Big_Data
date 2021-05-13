knitr::opts_chunk$set(echo = TRUE)

library(disk.frame)

setup_disk.frame()

# Read from a csv file

mem <- csv_to_disk.frame(infile = "./mem_1000.csv")

class(mem)

mem %>% 
  head()

# Read csv file in chunks

mem_cc <-
csv_to_disk.frame(infile = "./mem_1000.csv", 
                  in_chunk_size = 100,
                  colClasses = list(character=1:45))

class(mem_cc)

head(mem_cc)

# Read a zipped csv file

mem_z <- zip_to_disk.frame(zipfile = "./mem_1000.zip", outdir = tempdir())

class(mem_z)
length(mem_z)

mem_z[[1]] %>% 
  head()

# Read zipped csv in chunks

mem_zc <-
zip_to_disk.frame(zipfile = "./mem_1000.zip", 
                  outdir = tempdir(), 
                  in_chunk_size = 100,
                  colClasses = list(character=1:45))

class(mem_zc)
length(mem_zc)
mem_zc[[1]] %>% head()
